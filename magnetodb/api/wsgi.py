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
import shlex
import string

import routes

from magnetodb.common import wsgi, setup_global_env, is_global_env_ready

from magnetodb.api.amz import controller as amz_api_controller
from magnetodb.api.amz import wsgi as amazon_wsgi


class MagnetoDBApplication(wsgi.Router):

    """API"""
    def __init__(self):
        mapper = routes.Mapper()
        super(MagnetoDBApplication, self).__init__(mapper)

        amz_dynamodb_api_app = (
            amazon_wsgi.AmazonResource(
                controller=amz_api_controller.AmzDynamoDBApiController())
        )

        mapper.connect("/", controller=amz_dynamodb_api_app,
                       conditions={'method': 'POST'},
                       action="process_request")

    @classmethod
    def factory_method(cls, global_conf, **local_conf):
        if not is_global_env_ready():
            options = dict(global_conf.items() + local_conf.items())
            oslo_config_args = options.get("oslo_config_args")
            s = string.Template(oslo_config_args)
            oslo_config_args = shlex.split(s.substitute(**options))

            setup_global_env(
                program=options.get("program", "magnetodb-api"),
                args=oslo_config_args
            )
        return cls()
