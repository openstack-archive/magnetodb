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

import json

from keystoneclient.generic import client

from magnetodb.common import setup_global_env
from magnetodb import storage
from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)

STATUS_OK = 'OK'
STATUS_ERROR = 'ERROR'

keystoneclient = client.Client()


class HealthCheckApp(object):
    """ Controller for health check request. """

    def __init__(self, auth_uri=''):
        super(HealthCheckApp, self).__init__()
        self.auth_uri = auth_uri

    def __call__(self, environ, start_response):
        path = environ['PATH_INFO']

        LOG.debug('Request received: %s', path)

        if path:
            start_response('404 Not found', [('Content-Type', 'text/html')])
            return 'Incorrect url. Please check it and try again\n'

        resp = {'status': STATUS_OK}
        cas_status_msg = 'Cassandra: {}'
        key_status_msg = 'Keystone: {}'
        try:
            storage.health_check()
        except Exception:
            resp['status'] = STATUS_ERROR
            cas_status_msg = cas_status_msg.format(STATUS_ERROR)
        else:
            cas_status_msg = cas_status_msg.format(STATUS_OK)
        try:
            keystoneclient.discover(self.auth_uri)
        except Exception:
            resp['status'] = STATUS_ERROR
            key_status_msg = key_status_msg.format(STATUS_ERROR)
        else:
            key_status_msg = key_status_msg.format(STATUS_OK)
        resp['details'] = ', '.join([cas_status_msg, key_status_msg])

        start_response('200 OK', [('Content-Type', 'application/json')])
        return json.dumps(resp)


@setup_global_env(default_program='magnetodb-streaming-api')
def app_factory(global_conf, **local_conf):
    return HealthCheckApp(global_conf.get('auth_uri', ''))
