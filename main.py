# -*- coding: utf-8 -*-
# Copyright (c) 2016, Data Exploration Lab
# Distributed under the terms of the Modified BSD License.

from collections import namedtuple
import errno
import datetime
from distutils.version import StrictVersion
import json
import logging
import os
import random
import re
import socket
import string
import subprocess
import uuid

import docker
import girder_client
from dateutil.parser import parse as parse_date
import tornado.ioloop
import tornado.web
from tornado import gen
from tornado.httpclient import HTTPRequest, HTTPError, AsyncHTTPClient

import dockworker

AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

API_VERSION = '1.1'
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
        logging.warn("Bad gider token")
        raise tornado.web.HTTPError(
            401, 'Failed to authenticate with girder'
        )

    # Allow sysop to delete any notebook
    userId = data.get('userId', user['_id'])
    if userId != user['_id'] and user["admin"]:
        user = gc.get("/user/{id}".format(id=userId))
        logging.info("Overriding user %s", user["login"])

    logging.debug("USER = %s", json.dumps(user))

    frontendId = data.get('frontendId')
    frontend = None
    if frontendId is not None:
        frontend = gc.get('/frontend/%s' % frontendId)
    return gc, user, frontend


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
        api_check = payload.get('api_version', '1.0')
        if StrictVersion(api_check) != StrictVersion(API_VERSION):
            raise tornado.web.HTTPError(
                500, 'Unsupported API (%s) (server API %s)'
                % (api_check, API_VERSION))
        gc, user, frontend = yield parse_request_body(payload)

        vol_name = "%s_%s" % (payload['folderId'], user['login'])
        cli = docker.Client(base_url=DOCKER_URL)
        volume = cli.create_volume(name=vol_name, driver='local')
        logging.info("Volume: %s created", vol_name)
        logging.info("Mountpoint: %s", volume['Mountpoint'])

        homeDir = gc.loadOrCreateFolder('Notebooks', user['_id'], 'user')
        items = [item["_id"] for item in gc.listItem(homeDir['_id'])
                 if item["name"].endswith("pynb")]
        # TODO: should be done in one go with /resource endpoint
        #  but client doesn't have it yet
        for item in items:
            gc.downloadItem(item, HOSTDIR + volume["Mountpoint"])

        # TODO: read uid/gid from env/config
        for item in os.listdir(HOSTDIR + volume["Mountpoint"]):
            if item == 'data':
                continue
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

        container_config = yield self.get_container_config(frontend)
        # CREATE CONTAINER
        # REGISTER CONTAINER WITH PROXY
        container = yield self._launch_container(
            volume, container_config=container_config)

        self.write(dict(mountPoint=volume['Mountpoint'],
                        containerId=container.id,
                        containerPath=container.path,
                        host=container.host))
        self.finish()

    @gen.coroutine
    def get_container_config(self, frontend):
        if frontend is None:
            container_config = self.settings['container_config']
        else:
            defaults = self.settings['container_config']
            command = frontend.get('command') or defaults.command
            image = frontend['imageName']
            mem_limit = frontend.get('memLimit') or defaults.mem_limit
            port = frontend.get('port') or defaults.container_port
            user = frontend.get('user') or defaults.container_user
            cpu_shares = frontend.get('cpuShares') or defaults.cpu_shares

            container_config = dockworker.ContainerConfig(
                command=command,
                image=image,
                mem_limit=mem_limit,
                cpu_shares=cpu_shares,
                container_ip=defaults.container_ip,
                container_port=port,
                container_user=user,
                host_network=bool(os.environ.get('HOST_NETWORK', False)),
                host_directories=None,
                extra_hosts=[]
            )
        raise gen.Return(container_config)

    @gen.coroutine
    def _launch_container(self, volume, container_config=None):
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
        if container_config is None:
            container_config = self.container_config
        nb_token = uuid.uuid4().hex
        create_result = yield self.spawner.create_notebook_server(
            base_path=path, container_name=container_name,
            container_config=container_config,
            volume_bindings=volume_bindings,
            security_token=nb_token,
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

        container = PooledContainer(
            id=container_id, path='%s/login?token=%s' % (path, nb_token),
            host=host_ip)
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
                                        host=payload['host'])
        except KeyError:
            raise tornado.web.HTTPError(
                500, 'Got incomplete request from Girder')

        gc, user, _ = yield parse_request_body(payload)

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
        homeDir = gc.loadOrCreateFolder('Notebooks', user['_id'], 'user')
        try:
            gc.upload('{}/*.ipynb'.format(HOSTDIR + payload["mountPoint"]),
                      homeDir['_id'], reuseExisting=True, blacklist=["data"])
        except girder_client.HttpError as err:
            logging.warn("Something went wrong with data upload: %s" %
                         err.responseText)
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
        ' --NotebookApp.token=""'
        ' --NotebookApp.base_url=/{base_path}'
        ' --NotebookApp.port_retries=0'
    )
    container_image = os.environ.get(
        'DOCKER_IMAGE', 'xarthisius/singleanonuser')
    container_memlimit = os.environ.get(
        'DOCKER_MEMLIMIT', '2048m')
    container_ip = os.environ.get(
        'DOCKER_GATEWAY', '172.17.0.1')

    # TODO: read from env / config file
    container_config = dockworker.ContainerConfig(
        command=command_default,
        image=container_image,
        mem_limit=container_memlimit,
        cpu_shares=None,
        container_ip=container_ip,
        container_port='8888',
        container_user='jovyan',
        host_network=bool(os.environ.get('HOST_NETWORK', False)),
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
