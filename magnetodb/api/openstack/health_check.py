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

from magnetodb.api import with_global_env
from magnetodb.common import exception
from magnetodb.common import config
from magnetodb import storage
from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)

keystoneclient = client.Client()
CONF = config.CONF


def check_identity(auth_uri=''):
    if not keystoneclient.discover(auth_uri):
        raise exception.MagnetoError("Keystone discover failure")


def check_storage():
    storage.health_check()


def check_queue():
    url = (
        'amqp://{}:{}@{}:{}//'
    ).format(
        CONF.rabbit_userid,
        CONF.rabbit_password,
        CONF.rabbit_host,
        CONF.rabbit_port
    )
    with kombu.Connection(url) as conn:
        simple_queue = conn.SimpleQueue('simple_queue')
        message = 'Healthcheck sent'
        simple_queue.put(message)
        LOG.debug('Sent: %s' % message)

        message = simple_queue.get(block=True, timeout=1)
        LOG.debug("Received: %s" % message.payload)
        message.ack()
        simple_queue.close()


class HealthCheckApp(object):
    """ Controller for health check request. """
    STATUS_OK = '200'
    STATUS_ERROR = '503'
    error_msg = 'ERROR'
    success_msg = 'OK'

    default_exception = Exception

    def __init__(self, auth_uri=''):
        super(HealthCheckApp, self).__init__()
        self.subsystem_check_tuple = (
            ("Identity", check_identity, (auth_uri,), exception.MagnetoError),
            ("Storage", check_storage, (),
             exception.BackendInteractionException),
            ("Messaging", check_queue, (), self.default_exception),
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
        for subsystem, check, args, ex in self.subsystem_check_tuple:
            try:
                check(*args)
            except ex:
                results[subsystem] = self.error_msg
            else:
                results[subsystem] = self.success_msg

        return results


@with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return HealthCheckApp(global_conf.get('auth_uri', ''))
