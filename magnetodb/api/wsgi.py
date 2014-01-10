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
import shlex

from magnetodb.openstack.common import gettextutils
from magnetodb.openstack.common import log
from magnetodb.common import wsgi
from magnetodb.common import PROJECT_NAME

from magnetodb import storage

from magnetodb.api.amz import controller as amz_api_controller
from magnetodb.api.amz import wsgi as amazon_wsgi

from magnetodb.common import config

gettextutils.install(PROJECT_NAME, lazy=False)


class MagnetoDBApplication(wsgi.Router):

    """API"""
    def __init__(self, **options):
        args = []
        for k, v in options.iteritems():
            args.append(k)
            args.append(v)

        config.parse_args(
            prog=options.get("program", "magnetodb-api"),
            args=args
        )
        log.setup(PROJECT_NAME)
        storage.setup()

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
        return cls(**local_conf)
