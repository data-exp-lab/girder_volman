import re
import random
import socket
import errno
import string
import logging
import tornado.ioloop
import tornado.web
from tornado import gen
from collections import namedtuple
import json
import girder_client
import libmount
import docker
import os
import dockworker
import tinydb
from tornado.httpclient import HTTPRequest, HTTPError, AsyncHTTPClient

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

GIRDER_API_URL = os.environ.get(
    "GIRDER_API_URL", "https://girder.hub.yt/api/v1")
DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")

MOUNTS = {}

PooledContainer = namedtuple('PooledContainer', ['id', 'path'])


def sample_with_replacement(a, size):
    '''Get a random path. If Python had sampling with replacement built in,
    I would use that. The other alternative is numpy.random.choice, but
    numpy is overkill for this tiny bit of random pathing.'''
    return "".join([random.SystemRandom().choice(a) for x in range(size)])


def new_user(size):
    return sample_with_replacement(string.ascii_letters + string.digits, size)


@gen.coroutine
def download_items(gc, folder_id, dest):
    '''Download all items from a girder folder

    Parameters
    ----------

    gc : GirderClient
        Initiliazed instance of GirderClient
    folder_id : str
        Girder's folder id
    dest : str
        Destination path
    '''
    items = gc.listResource('/item', {'folderId': folder_id, 'limit': 200})
    for item in items:
        logging.info("[=] downloading %s", item["name"])
        gc.downloadItem(item["_id"], dest)
        logging.info("[=] finished downloading %s", item["name"])


class MainHandler(tornado.web.RequestHandler):

    @property
    def db(self):
        return self.settings['mount_db']

    @property
    def proxy_token(self):
        return self.settings['proxy_token']

    @property
    def pool_name(self):
        return self.settings['pool_name']

    @property
    def proxy_endpoint(self):
        return self.settings['proxy_endpoint']

    @property
    def container_config(self):
        return self.settings['container_config']

    @property
    def spawner(self):
        return self.settings['spawner']

    @property
    def container_name_pattern(self):
        return self.settings['container_name_pattern']

    @gen.coroutine
    def post(self):
        body = json.loads(self.request.body.decode("utf-8"))
        girder_token = body['girder_token']
        folder_id = body['collection_id']

        gc = girder_client.GirderClient(apiUrl=GIRDER_API_URL)
        logging.info("got token: %s, folder_id: %s" %
                     (girder_token, folder_id))
        gc.token = girder_token
        user = gc.get("/user/me")
        if user is None:
            logging.warn("Bad gider token")
            raise tornado.web.HTTPError(
                401, 'Failed to authenticate with girder'
            )

        logging.debug("USER = %s", json.dumps(user))
        username = user["login"]
        db_entry = {'mounts': [], 'folder_id': folder_id,
                    'username': username}

        vol_name = "%s_%s" % (folder_id, username)
        cli = docker.Client(base_url=DOCKER_URL)
        volume = cli.create_volume(name=vol_name, driver='local')
        logging.info("Volume: %s created", vol_name)
        logging.info("Mountpoint: %s", volume['Mountpoint'])

        params = {'parentType': 'user', 'parentId': user["_id"],
                  'name': 'Private'}
        homeDir = gc.listResource("/folder", params)[0]["_id"]
        gc.downloadFolderRecursive(homeDir, volume["Mountpoint"])

        # TODO: read uid/gid from env/config
        for item in os.listdir(volume["Mountpoint"]):
            os.chown(os.path.join(volume["Mountpoint"], item), 1000, 100)

        dest = os.path.join(volume["Mountpoint"], "data")
        try:
            os.mkdir(dest)
        except OSError as e:
            if e.errno != 17:
                raise
            pass

        db_entry["mount_point"] = volume["Mountpoint"]

        params = {'parentType': 'folder', 'parentId': folder_id,
                  'limit': 200}
        folders = gc.listResource("/folder", params)
        for folder in folders:
            sizeGB = folder.get("size", 0) // 1024**3
            metadata = folder.get("meta", None)
            logging.info("Metadata for folder", metadata)
            if metadata is not None:
                source = metadata.get("phys_path", None)
            else:
                source = None

            if source is not None:
                logging.info("[*] Using mount bind for %s", source)
                target = os.path.join(dest, os.path.basename(source))
                try:
                    os.mkdir(target)
                except OSError as e:
                    if e.errno != 17:
                        raise
                    pass

                cx = libmount.Context()
                cx.fstype = "bind"
                cx.options = "bind"
                logging.info("[*] Source: %s target: %s", source, target)
                cx.target = "{}".format(target)
                cx.source = "{}".format(source)
                cx.mount()
                db_entry['mounts'].append(target)
                logging.info("[*] %s binded to %s", source, target)
            else:
                # TODO
                # this doesn't work, as girder doesn't report size properly
                if sizeGB > 1:
                    logging.info("[*] folder is too big to download: %i GB",
                                 sizeGB)
                    continue

                logging.info("[=] downloading recursively %s", folder_id)
                # start girder download, since it may take some time we are
                # using background task to download data from girder, there's
                # high chance it'll be finished before user actually needs
                # anything
                tornado.ioloop.IOLoop.current().spawn_callback(
                    gc.downloadFolderRecursive, folder["_id"],
                    os.path.join(dest, folder["name"])
                )
                logging.info("[=] finished downloading %s", folder_id)

        # asynchronously download all items
        tornado.ioloop.IOLoop.current().spawn_callback(download_items,
                                                       gc, folder_id, dest)

        # CREATE CONTAINER
        # REGISTER CONTAINER WITH PROXY
        container = yield self._launch_container(volume)
        db_entry['container_id'] = container.id
        db_entry['container_path'] = container.path
        self.db.insert(db_entry)
        self.write({'url': '/{}'.format(container.path)})
        self.finish()

    @gen.coroutine
    def _launch_container(self, volume):
        user = new_user(12)
        path = "user/" + user
        container_name = 'tmp.{}.{}'.format(self.pool_name, user)
        volume_bindings = {volume["Name"]: {
            'bind': "/home/jovyan/work", 'mode': 'rw'}}
        if not self.container_name_pattern.match(container_name):
            pattern = self.container_name_pattern.pattern
            raise Exception("[{}] does not match [{}]!".format(container_name,
                                                               pattern))

        logging.info("Launching new notebook server [%s] at path [%s].",
                     container_name, path)
        create_result = yield self.spawner.create_notebook_server(
            base_path=path, container_name=container_name,
            container_config=self.container_config,
            volume_bindings=volume_bindings
        )
        container_id, host_ip, host_port = create_result
        logging.info(
            "Created notebook server [%s] for path [%s] at [%s:%s]",
            container_name, path, host_ip, host_port)

        # Wait for the server to launch within the container before adding it
        # to the pool or serving it to a user.

        yield self._wait_for_server(host_ip, host_port, path)

        http_client = AsyncHTTPClient()
        headers = {"Authorization": "token {}".format(self.proxy_token)}

        proxy_endpoint = "{}/api/routes/{}".format(self.proxy_endpoint, path)
        body = json.dumps({
            "target": "http://{}:{}".format(host_ip, host_port),
            "container_id": container_id,
        })

        logging.debug("Proxying path [%s] to port [%s].", path, host_port)
        req = HTTPRequest(proxy_endpoint, method="POST", headers=headers,
                          body=body)
        try:
            yield http_client.fetch(req)
            logging.info("Proxied path [%s] to port [%s].", path, host_port)
        except HTTPError as e:
            logging.error("Failed to create proxy route to [%s]: %s", path, e)

        container = PooledContainer(id=container_id, path=path)
        raise gen.Return(container)

    @gen.coroutine
    def _wait_for_server(self, ip, port, path, timeout=10, wait_time=0.2):
        '''Wait for a server to show up within a newly launched container.'''

        logging.info("Waiting for a container to launch at [%s:%s].", ip, port)
        loop = tornado.ioloop.IOLoop.current()
        tic = loop.time()

        # Docker starts listening on a socket before the container is fully
        # launched. Wait for that, first.

        while loop.time() - tic < timeout:
            try:
                socket.create_connection((ip, port))
            except socket.error as e:
                logging.warn("Socket error on boot: %s", e)
                if e.errno != errno.ECONNREFUSED:
                    logging.warn("Error attempting to connect to [%s:%i]: %s",
                                 ip, port, e)
                yield gen.Task(loop.add_timeout, loop.time() + wait_time)
            else:
                break

        # Fudge factor of IPython notebook bootup.
        # TODO: Implement a webhook in IPython proper to call out when the
        # notebook server is booted.
        yield gen.Task(loop.add_timeout, loop.time() + .5)

        # Now, make sure that we can reach the Notebook server.
        http_client = AsyncHTTPClient()
        req = HTTPRequest("http://{}:{}/{}".format(ip, port, path))

        while loop.time() - tic < timeout:
            try:
                yield http_client.fetch(req)
            except HTTPError as http_error:
                code = http_error.code
                logging.info(
                    "Booting server at [%s], getting HTTP status [%s]",
                    path, code)
                yield gen.Task(loop.add_timeout, loop.time() + wait_time)
            else:
                break

        logging.info("Server [%s] at address [%s:%s] has booted! Have at it.",
                     path, ip, port)

    @gen.coroutine
    def _proxy_remove(self, path):
        '''Remove a path from the proxy.'''

        url = "{}/api/routes/{}".format(self.proxy_endpoint, path.lstrip('/'))
        headers = {"Authorization": "token {}".format(self.proxy_token)}
        req = HTTPRequest(url, method="DELETE", headers=headers)
        http_client = AsyncHTTPClient()

        try:
            yield http_client.fetch(req)
        except HTTPError as e:
            logging.error("Failed to delete route [%s]: %s", path, e)

    @gen.coroutine
    def delete(self):
        body = json.loads(self.request.body.decode("utf-8"))
        girder_token = body['girder_token']
        folder_id = body['collection_id']

        gc = girder_client.GirderClient(apiUrl=GIRDER_API_URL)
        logging.debug("got token: %s, folder_id: %s" %
                      (girder_token, folder_id))
        gc.token = girder_token
        user = gc.get("/user/me")
        username = user["login"]
        logging.debug("Username %s", username)

        query = tinydb.Query()
        data = self.db.search((query.username == username) &
                              (query.folder_id == folder_id))

        try:
            db_entry = data[0]
        except IndexError:
            self.finish()

        container = PooledContainer(id=db_entry["container_id"],
                                    path=db_entry["container_path"])
        try:
            logging.info("Releasing container [%s].", container)
            yield [
                self.spawner.shutdown_notebook_server(container.id),
                self._proxy_remove(container.path)
            ]
            logging.debug("Container [%s] has been released.", container)
        except Exception as e:
            logging.error("Unable to release container [%s]: %s", container, e)
            self.finish()

        vol_name = "%s_%s" % (folder_id, username)
        for mount_point in db_entry['mounts']:
            logging.info("Unmounting %s", mount_point)
            cx = libmount.Context()
            cx.target = mount_point
            cx.umount()

        # upload notebooks
        user_id = gc.get("/user/me")["_id"]
        params = {'parentType': 'user', 'parentId': user_id,
                  'name': 'Private'}
        homeDir = gc.listResource("/folder", params)[0]["_id"]
        gc.blacklist.append("data")
        gc.upload('{}/*.ipynb'.format(db_entry["mount_point"]),
                  homeDir, reuse_existing=True)

        cli = docker.Client(base_url=DOCKER_URL)
        logging.info("Removing volume: %s", vol_name)
        cli.remove_volume(vol_name)

        self.db.remove((query.username == username) &
                       (query.folder_id == folder_id))

    def get(self):
        self.write("Hello, world\n")

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    handlers = [
        (r"/", MainHandler),
    ]

    docker_host = os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')
    mount_db = tinydb.TinyDB(os.environ.get('MOUNT_DB', 'mounts.json'))

    command_default = (
        'jupyter notebook --no-browser'
        ' --port {port} --ip=0.0.0.0'
        ' --NotebookApp.base_url=/{base_path}'
        ' --NotebookApp.port_retries=0'
    )

    # TODO: read from env / config file
    container_config = dockworker.ContainerConfig(
        command=command_default,
        image="tmpnb-notebook",
        mem_limit="1024m",
        cpu_shares=None,
        container_ip='172.17.0.1',
        container_port='8888',
        container_user='jovyan',
        host_network=False,
        host_directories=None,
        extra_hosts=[]
    )

    spawner = dockworker.DockerSpawner(docker_host,
                                       timeout=30,
                                       version="auto",
                                       max_workers=4,
                                       assert_hostname=False,
                                       )

    settings = dict(
        spawner=spawner,
        container_name_pattern=re.compile('tmp\.([^.]+)\.(.+)\Z'),
        pool_name="tmpnb",
        mount_db=mount_db,
        container_config=container_config,
        proxy_token=os.environ.get('CONFIGPROXY_AUTH_TOKEN', "devtoken"),
        proxy_endpoint=os.environ.get(
            'CONFIGPROXY_ENDPOINT', "http://127.0.0.1:8001"),
    )
    app = tornado.web.Application(handlers, **settings)
    app.listen(9005)
    tornado.ioloop.IOLoop.current().start()