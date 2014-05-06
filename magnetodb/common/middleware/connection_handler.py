# Copyright 2014 Mirantis Inc.
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

from magnetodb.common import wsgi

from magnetodb.openstack.common import log as logging

logger = logging.getLogger(__name__)

HTTP_CONNECTION = "Connection"


class ConnectionHandler(wsgi.Middleware):

    def __init__(self, app, options):
        self.options = options
        super(ConnectionHandler, self).__init__(app)

    def process_request(self, req):
        response = req.get_response(self.application)
        connection = req.headers.get(HTTP_CONNECTION, None)
        if connection:
            response.headers[HTTP_CONNECTION] = connection
        return response


def factory_method(global_config, **local_config):
    return lambda application: ConnectionHandler(application, local_config)
