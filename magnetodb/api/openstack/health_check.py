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

import urlparse
import kombu

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


def _check_rabbit():
    url = (
        'amqp://{}:{}@{}:{}//'
    ).format(
        CONF.rabbit_userid,
        CONF.rabbit_password,
        CONF.rabbit_host,
        CONF.rabbit_port
    )
    try:
        with kombu.Connection(url) as conn:
            simple_queue = conn.SimpleQueue('simple_queue')
            message = 'Healthcheck sent'
            simple_queue.put(message)
            LOG.debug('Sent: %s' % message)

            message = simple_queue.get(block=True, timeout=1)
            LOG.debug("Received: %s" % message.payload)
            message.ack()
            simple_queue.close()
    except Exception:
        return False

    return True


class SubsystemCheck(object):

    def check(self):
        return NotImplemented


class IdentityCheck(SubsystemCheck):

    name = "Keystone"

    def __init__(self, auth_uri=''):
        super(IdentityCheck, self).__init__()
        self.auth_uri = auth_uri

    def check(self):
        return _check_keystone(self.auth_uri)


class StorageCheck(SubsystemCheck):

    name = "Cassandra"

    def check(self):
        try:
            storage.health_check()
        except exception.BackendInteractionError:
            return False
        return True


class MessagingCheck(SubsystemCheck):

    RPC_NAME_MAP = {
        "rabbit": "RabbitMQ",
    }
    RPC_CHECK_MAP = {
        "rabbit": _check_rabbit,
    }

    def __init__(self):
        super(MessagingCheck, self).__init__()
        self._rpc_backend = CONF.rpc_backend
        self.name = self.RPC_NAME_MAP.get(
            self._rpc_backend, self._rpc_backend
        )

    def check(self):
        check = self.RPC_CHECK_MAP.get(self._rpc_backend, None)
        if check:
            return check()
        else:
            raise NotImplementedError()


class HealthCheckApp(object):
    """Controller for health check request."""

    STATUS_OK = '200'
    STATUS_ERROR = '503'
    error_msg = 'ERROR'
    success_msg = 'OK'
    not_implemented_msg = 'Not Implemented'

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
            start_response('404 Not found', [('Content-Type', 'text/plain')])
            return 'Incorrect url. Please check it and try again\n'

        resp = 'API: ' + self.success_msg
        error_status = self.STATUS_OK

        query_string = environ.get('QUERY_STRING', '')
        fullcheck = urlparse.parse_qs(query_string).get('fullcheck')
        subsystems_status = {}
        if (isinstance(fullcheck, list) and ('true' in fullcheck or
                                             'True' in fullcheck)):
            subsystems_status = self._check_subsystems()

        if subsystems_status:
            resp = ', '.join([resp, self._format_resp(subsystems_status)])
        if self.error_msg in subsystems_status.values():
            error_status = self.STATUS_ERROR

        resp += '\n'
        start_response(error_status, [('Content-Type', 'text/plain')])
        return resp

    def _format_resp(self, body):
        messages = []
        for k, v in body.iteritems():
            messages.append(": ".join([k, v]))
        return ", ".join(messages)

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
