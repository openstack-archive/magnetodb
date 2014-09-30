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

import urlparse

from keystoneclient.generic import client

from magnetodb.api import with_global_env
from magnetodb.common import exception
from magnetodb import storage
from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)

STATUS_OK = '200'
STATUS_ERROR = '503'

keystoneclient = client.Client()


class HealthCheckApp(object):
    """ Controller for health check request. """

    def __init__(self, auth_uri=''):
        super(HealthCheckApp, self).__init__()
        self.auth_uri = auth_uri

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']

        LOG.debug('Request received: %s', path)

        if path and path != '/':
            start_response('404 Not found', [('Content-Type', 'text/plain')])
            return 'Incorrect url. Please check it and try again\n'

        resp = ''
        query_string = environ.get('QUERY_STRING', '')
        fullcheck = urlparse.parse_qs(query_string).get('fullcheck')

        if isinstance(fullcheck, list) and 'true' in fullcheck:
            resp = self._check_subsystems()

        if not resp:
            error_status = STATUS_OK
            resp = 'OK'
        else:
            error_status = STATUS_ERROR

        resp += '\n'
        start_response(error_status, [('Content-Type', 'text/plain')])
        return resp

    def _check_subsystems(self):
        cas_error_msg = 'Cassandra: ERROR'
        key_error_msg = 'Keystone: ERROR'
        resp = ''
        try:
            storage.health_check()
        except exception.BackendInteractionException:
            resp = cas_error_msg

        if not keystoneclient.discover(self.auth_uri):
            if resp:
                resp = '. '.join([resp, key_error_msg])
            else:
                resp = key_error_msg
        return resp


@with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return HealthCheckApp(global_conf.get('auth_uri', ''))
