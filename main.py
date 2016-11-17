# -*- coding: utf-8 -*-
# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

import base64
from collections import namedtuple
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.exceptions import InvalidSignature
import errno
import datetime
import json
import logging
import os
import random
import re
import shutil
import socket
import string
import subprocess
import tempfile

import docker
import girder_client
from dateutil.parser import parse as parse_date
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.httpclient import HTTPRequest, HTTPError, AsyncHTTPClient

import dockworker

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

GIRDER_API_URL = os.environ.get(
    "GIRDER_API_URL", "https://girder.hub.yt/api/v1")
DOCKER_URL = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
HOSTDIR = os.environ.get("HOSTDIR", "/host")
MAX_FILE_SIZE = os.environ.get("MAX_FILE_SIZE", 200)

MOUNTS = {}

PooledContainer = namedtuple('PooledContainer', ['id', 'path', 'host'])


def sample_with_replacement(a, size):
    '''Get a random path. If Python had sampling with replacement built in,
    I would use that. The other alternative is numpy.random.choice, but
    numpy is overkill for this tiny bit of random pathing.'''
    return "".join([random.SystemRandom().choice(a) for x in range(size)])


def new_user(size):
    return sample_with_replacement(string.ascii_letters + string.digits, size)


def _safe_mkdir(dest):
    try:
        os.mkdir(dest)
    except OSError as e:
        if e.errno != 17:
            raise
        logging.warn("Failed to mkdir {}".format(dest))
        pass


def verify_message(gc, payload):
    hub_data = gc.get('/ythub')
    hub_public_key = serialization.load_pem_public_key(
        hub_data['pubkey'].encode('utf8'),
        backend=default_backend()
    )

    signature = payload.pop('signature', None)
    if signature is None:
        return False

    verifier = hub_public_key.verifier(
        base64.b64decode(signature.encode('utf8')),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

    verifier.update(json.dumps(payload).encode('utf8'))
    try:
        verifier.verify()
    except InvalidSignature:
        return False
    return True


@gen.coroutine
def get_home_dir(gc):
    nb_store = {'parentType': 'user', 'parentId': gc.get("/user/me")["_id"],
                'name': 'Notebooks'}
    homeDir = list(gc.listResource("/folder", nb_store))
    if homeDir:
        return homeDir[0]
    else:
        return gc.createFolder(
            nb_store['parentId'], nb_store['name'],
            parentType=nb_store['parentType'], public=False)


@gen.coroutine
def cull_idle(proxy_url, proxy_token, timeout):
    cull_limit = datetime.datetime.utcnow() \
        - datetime.timedelta(seconds=timeout)

    http_client = AsyncHTTPClient()
    logging.debug("Polling proxy for idle containers")
    req = HTTPRequest(proxy_url + '/api/users',
                      headers={"Authorization": "token %s" % proxy_token})
    try:
        resp = yield http_client.fetch(req)
    except HTTPError as e:
        logging.error("Failed to poll proxy for idle containers: %s", e)

    users = json.loads(resp.body.decode('utf8', 'replace'))
    futures = []
    for user in users:
        last_activity = parse_date(user['last_activity'])
        if user['server'] and last_activity < cull_limit:
            logging.info(
                "Culling %s (inactive since %s)", user['name'], last_activity)
            # req = HTTPRequest(url=url+'/api/users/%s/server' % user['name'],
            #                  method='DELETE',
            #                  headers=auth_header,
            #                  )
            futures.append((user['name'], http_client.fetch(req)))
        elif user['server'] and last_activity > cull_limit:
            logging.debug("Not culling %s (active since %s)",
                          user['name'], last_activity)

    for (name, f) in futures:
        yield f
        logging.debug("Finished culling %s", name)


@gen.coroutine
def _get_api_key(gc):
    api_key = None
    for key in gc.get('/api_key'):
        if key['name'] == 'tmpnb' and key['active']:
            api_key = key['key']

    if api_key is None:
        api_key = gc.post('/api_key',
                          data={'name': 'tmpnb', 'active': True})['key']
    return api_key


@gen.coroutine
def parse_request_body(data):
    gc = girder_client.GirderClient(apiUrl=GIRDER_API_URL)
    gc.token = data['girder_token']
    user = gc.get("/user/me")
    if user is None:
        logging.warn("Bad girder token")
        raise tornado.web.HTTPError(
            401, 'Failed to authenticate with girder'
        )

    # Allow sysop to delete any notebook
    userId = data.get('userId', user['_id'])
    if userId != user['_id'] and user["admin"]:
        user = gc.get("/user/{id}".format(id=userId))
        logging.info("Overriding user %s", user["login"])

    logging.debug("USER = %s", json.dumps(user))
    return gc, user


class MainHandler(tornado.web.RequestHandler):

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
        payload = json.loads(self.request.body.decode("utf-8"))
        gc, user = yield parse_request_body(payload)

        vol_name = "%s_%s" % (payload['folderId'], user['login'])
        cli = docker.Client(base_url=DOCKER_URL)
        volume = cli.create_volume(name=vol_name, driver='local')
        # TODO: any error raised beyond this point should be caught and
        # volume should be gracefully removed
        logging.info("Volume: %s created", vol_name)
        logging.info("Mountpoint: %s", volume['Mountpoint'])

        homeDir = yield get_home_dir(gc)
        items = [item['_id'] for item in gc.listItem(homeDir['_id'])
                 if item['name'].endswith('pynb')]
        # gc.downloadItem() uses NamedTemporaryFile() which in turn uses
        # os.rename() to move a file from tmpdir to the destination path.
        # Since we are moving files to mount binded host filesystem it raises
        # OSError: [Errno 18] Invalid cross-device link.
        # As a precaution we download notebooks directly to tmpdir and
        # subsequently move it to Mountpoint using shutil.move()
        notebooks_dir = tempfile.mkdtemp()
        for item in items:
            # TODO: should be done in one go with /resource endpoint
            #  but client doesn't have it yet
            gc.downloadItem(item, notebooks_dir)
        for notebook in os.listdir(notebooks_dir):
            shutil.move(os.path.join(notebooks_dir, notebook),
                        HOSTDIR + volume["Mountpoint"])
        os.rmdir(notebooks_dir)

        # TODO: read uid/gid from env/config
        for item in os.listdir(HOSTDIR + volume["Mountpoint"]):
            os.chown(os.path.join(HOSTDIR + volume["Mountpoint"], item),
                     1000, 100)

        dest = os.path.join(volume["Mountpoint"], "data")
        _safe_mkdir(HOSTDIR + dest)

        # FUSE is silly and needs to have mirror inside container
        if not os.path.isdir(dest):
            os.makedirs(dest)
        api_key = yield _get_api_key(gc)
        cmd = "girderfs -c direct --api-url {} --api-key {} {} {}".format(
            GIRDER_API_URL, api_key, dest, payload['folderId'])
        logging.info("Calling: %s", cmd)
        subprocess.call(cmd, shell=True)

        # CREATE CONTAINER
        # REGISTER CONTAINER WITH PROXY
        container = yield self._launch_container(volume)

        self.write(dict(mountPoint=volume['Mountpoint'],
                        containerId=container.id,
                        containerPath=container.path,
                        host=container.host))
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

        container = PooledContainer(id=container_id, path=path, host=host_ip)
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
        payload = json.loads(self.request.body.decode('utf-8'))

        try:
            container = PooledContainer(id=payload["containerId"],
                                        path=payload["containerPath"],
                                        host=payload["host"])
        except KeyError:
            raise tornado.web.HTTPError(
                500, 'Got incomplete request from Girder')

        gc, user = yield parse_request_body(payload)

        try:
            logging.info("Releasing container [%s].", container)
            yield [
                self.spawner.shutdown_notebook_server(container.id),
                self._proxy_remove(container.path)
            ]
            logging.info("Container [%s] has been released.", container)
        except Exception as e:
            logging.error("Unable to release container [%s]: %s", container, e)
            raise tornado.web.HTTPError(
                500, "Unable to remove container, contact admin")

        vol_name = "%s_%s" % (payload['folderId'], user['login'])
        dest = os.path.join(payload['mountPoint'], 'data')
        logging.info("Unmounting %s", dest)
        subprocess.call("umount %s" % dest, shell=True)

        # upload notebooks
        homeDir = yield get_home_dir(gc)
        if hasattr(gc, 'blacklist'):
            gc.blacklist.append("data")
        try:
            gc.upload('{}/*.ipynb'.format(HOSTDIR + payload["mountPoint"]),
                      homeDir['_id'], reuse_existing=True)
        except girder_client.HttpError:
            logging.warn("Something went wrong with data upload"
                         ", should backup data")
            pass  # upload failed, keep going

        cli = docker.Client(base_url=DOCKER_URL)
        try:
            logging.info("Removing volume: %s", vol_name)
            cli.remove_volume(vol_name)
        except Exception as e:
            logging.error("Unable to remove volume [%s]: %s", vol_name, e)
            pass

    @gen.coroutine
    def get(self):
        http_client = AsyncHTTPClient()
        logging.debug('Polling proxy for idle containers')
        headers = {'Authorization': 'token %s' % self.proxy_token}
        req = HTTPRequest(self.proxy_endpoint + '/api/routes', headers=headers)
        try:
            resp = yield http_client.fetch(req)
        except HTTPError as e:
            logging.error("Failed to poll proxy for idle containers: %s", e)
            raise tornado.web.HTTPError(
                400, 'Failed to connect to proxy'
            )

        proxy_entries = json.loads(resp.body.decode('utf8', 'replace'))
        result = {}
        for entry in proxy_entries.values():
            try:
                result[entry['container_id']] = entry['last_activity']
            except KeyError:
                pass
        self.write(json.dumps(result))
        self.finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    handlers = [
        (r"/", MainHandler),
    ]

    docker_host = os.environ.get('DOCKER_HOST', 'unix://var/run/docker.sock')

    command_default = (
        'jupyter notebook --no-browser'
        ' --port {port} --ip=0.0.0.0'
        ' --NotebookApp.base_url=/{base_path}'
        ' --NotebookApp.port_retries=0'
    )

    # TODO: read from env / config file
    container_config = dockworker.ContainerConfig(
        command=command_default,
        image="xarthisius/singleanonuser",
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
        container_config=container_config,
        proxy_token=os.environ.get('CONFIGPROXY_AUTH_TOKEN', "devtoken"),
        proxy_endpoint=os.environ.get(
            'CONFIGPROXY_ENDPOINT', "http://127.0.0.1:8001"),
    )
    app = tornado.web.Application(handlers, **settings)
    app.listen(9005)
    tornado.ioloop.IOLoop.current().start()
