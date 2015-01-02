# Copyright 2014 Symantec Corporation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
Health check request is a lightweight request that allows to check availability
of magnetodb-api service and its subsystems (Keystone, DB back-end, MQ).
"""

import json
import kombu
import urlparse

from keystoneclient.generic import client
from magnetodb import api
from magnetodb.common import config
from magnetodb.common import exception
from magnetodb.openstack.common import log as logging
from magnetodb import storage

LOG = logging.getLogger(__name__)
keystoneclient = client.Client()
CONF = config.CONF


def _check_keystone(auth_uri):
    return keystoneclient.discover(auth_uri)


def _test_messaging(url):
    with kombu.Connection(url, connect_timeout=1) as conn:
        simple_queue = conn.SimpleQueue('simple_queue')
        message = 'Healthcheck sent'
        simple_queue.put(message)
        LOG.debug('Sent: %s' % message)

        message = simple_queue.get(block=True, timeout=1)
        LOG.debug("Received: %s" % message.payload)
        message.ack()
        simple_queue.close()


def _check_rabbit():
    url_pattern = 'amqp://{}:{}@{}:{}//'

    rabbit_hosts = CONF.oslo_messaging_rabbit.rabbit_hosts
    urls = []
    if rabbit_hosts:
        for host in rabbit_hosts:
            hostname_port_pair = host.split(':')
            url = (
                url_pattern
            ).format(
                CONF.oslo_messaging_rabbit.rabbit_userid,
                CONF.oslo_messaging_rabbit.rabbit_password,
                hostname_port_pair[0],
                hostname_port_pair[1]
            )
            urls.append(url)
    else:
        url = (
            url_pattern
        ).format(
            CONF.oslo_messaging_rabbit.rabbit_userid,
            CONF.oslo_messaging_rabbit.rabbit_password,
            CONF.oslo_messaging_rabbit.rabbit_host,
            CONF.oslo_messaging_rabbit.rabbit_port
        )
        urls.append(url)

    healthy = True
    for url in urls:
        LOG.debug('Check messaging server at %s' % url)
        try:
            _test_messaging(url)
        except Exception:
            healthy = False
            break

    return healthy


def _check_qpid():
    url_pattern = 'amqp://{}:{}@{}:{}//'

    qpid_hosts = CONF.oslo_messaging_qpid.qpid_hosts
    urls = []
    if qpid_hosts:
        for host in qpid_hosts:
            hostname_port_pair = host.split(':')
            url = (
                url_pattern
            ).format(
                CONF.oslo_messaging_qpid.qpid_username,
                CONF.oslo_messaging_qpid.qpid_password,
                hostname_port_pair[0],
                hostname_port_pair[1]
            )
            urls.append(url)
    else:
        url = (
            url_pattern
        ).format(
            CONF.oslo_messaging_qpid.qpid_username,
            CONF.oslo_messaging_qpid.qpid_password,
            CONF.oslo_messaging_qpid.qpid_hostname,
            CONF.oslo_messaging_qpid.qpid_port
        )
        urls.append(url)

    healthy = True
    for url in urls:
        LOG.debug('Check messaging server at %s' % url)
        try:
            _test_messaging(url)
        except Exception:
            healthy = False
            break

    return healthy


class SubsystemCheck(object):

    def check(self):
        return NotImplemented


class IdentityCheck(SubsystemCheck):

    name = "Identity"

    def __init__(self, auth_uri=''):
        super(IdentityCheck, self).__init__()
        self.auth_uri = auth_uri

    def check(self):
        return _check_keystone(self.auth_uri)


class StorageCheck(SubsystemCheck):

    name = "Storage"

    def check(self):
        try:
            storage.health_check()
        except exception.BackendInteractionError:
            return False
        return True


class MessagingCheck(SubsystemCheck):

    name = "Messaging"

    RPC_CHECK_MAP = {
        "rabbit": _check_rabbit,
        "qpid": _check_qpid
    }

    def __init__(self):
        super(MessagingCheck, self).__init__()
        self._rpc_backend = CONF.rpc_backend

    def check(self):
        check = self.RPC_CHECK_MAP.get(self._rpc_backend, None)
        if check:
            return check()
        else:
            raise NotImplementedError()


class HealthCheckApp(object):
    """Controller for health check request."""

    STATUS_OK = '200'
    STATUS_NOTFOUND = '404'
    STATUS_ERROR = '503'
    error_msg = 'ERROR'
    success_msg = 'OK'
    not_implemented_msg = 'Not Implemented'
    not_found_msg = 'Not found'

    TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))

    def __init__(self, auth_uri=''):
        super(HealthCheckApp, self).__init__()
        self.subsystem_check_tuple = (
            IdentityCheck(auth_uri),
            StorageCheck(),
            MessagingCheck(),
        )

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']

        LOG.debug('Request received: %s', path)

        if path and path != '/':
            start_response(
                ' '.join((self.STATUS_NOTFOUND, self.not_found_msg)),
                [('Content-Type', 'text/plain')])
            return 'Incorrect url. Please check it and try again\n'

        resp = dict(API=self.success_msg)
        error_status = self.STATUS_OK

        query_string = environ.get('QUERY_STRING', '')
        fullcheck = urlparse.parse_qs(query_string).get('fullcheck')
        subsystems_status = {}
        if fullcheck and fullcheck[0] in self.TRUE_VALUES:
            subsystems_status = self._check_subsystems()

        if subsystems_status:
            resp.update(subsystems_status)
        resp = self._format_resp(resp)
        if self.error_msg in subsystems_status.values():
            error_status = self.STATUS_ERROR

        start_response(error_status, [('Content-Type', 'application/json')])
        return resp

    def _format_resp(self, body):
        return json.dumps(body)

    def _check_subsystems(self):
        results = {}
        for subsystem in self.subsystem_check_tuple:
            try:
                res = subsystem.check()
            except NotImplementedError:
                msg = self.not_implemented_msg
            else:
                if res:
                    msg = self.success_msg
                else:
                    msg = self.error_msg
            results[subsystem.name] = msg
        return results


@api.with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return HealthCheckApp(global_conf.get('auth_uri', ''))
