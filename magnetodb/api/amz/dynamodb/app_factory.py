# Copyright 2013 Mirantis Inc.
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

import routes

from magnetodb.api import amz
from magnetodb.api import with_global_env
from magnetodb.common import wsgi


class DynamoDBApiApplication(wsgi.Router):

    """API"""
    def __init__(self):
        mapper = routes.Mapper()
        super(DynamoDBApiApplication, self).__init__(mapper)

        amz_dynamodb_api_app = (
            amz.AmazonResource(
                controller=amz.AmzDynamoDBApiController())
        )

        mapper.connect("/", controller=amz_dynamodb_api_app,
                       conditions={'method': 'POST'},
                       action="process_request")


@with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return DynamoDBApiApplication()
