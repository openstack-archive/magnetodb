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

import re

import pecan

from oslo_context import context as req_context

from magnetodb import api

from magnetodb.api.amz import dynamodb
from magnetodb.api.amz import exception as amz_exception


class DynamoDBRootController(object):

    capabilities = {
        'DynamoDB': {
            '20111205': dynamodb.capabilities,
            '20120810': dynamodb.capabilities
        }
    }

    @pecan.expose(generic=True)
    def index(self):
        pecan.abort(404)

    # HTTP POST /
    @index.when(method='POST', content_type='application/x-amz-json-1.0',
                template="json")
    def index_post(self, **kw):
        target = pecan.request.environ['HTTP_X_AMZ_TARGET']

        if not target:
            raise (
                amz_exception.AWSValidationException(
                    "'x-amz-target' header wasn't found")
            )

        matcher = re.match("(\w+)_(\w+)\.(\w+)", target)

        if not matcher:
            raise (
                amz_exception.AWSValidationException(
                    "'x-amz-target' header wasn't recognized (actual: %s, "
                    "expected format: <<serviceName>>_<<API version>>."
                    "<<operationName>>")
            )
        service_name = matcher.group(1)
        api_version = matcher.group(2)
        action_name = matcher.group(3)

        return self.process_action(service_name, api_version, action_name,
                                   pecan.request.json_body)

    def process_action(self, service_name, api_version, action_name,
                       action_params):
        service_capabilities = self.capabilities.get(service_name, None)

        if service_capabilities is None:
            raise amz_exception.AWSBadRequestException(
                "Service '%s' isn't supported" % service_name)

        target_capabilities = service_capabilities.get(api_version, None)

        if target_capabilities is None:
            raise (
                amz_exception.AWSBadRequestException(
                    "Service '%s' doesn't support API version '%s'" %
                    (service_name, api_version))
            )

        action = target_capabilities.get(action_name, None)

        if action is None:
            raise (
                amz_exception.AWSValidationException(
                    "Service '%s', API version '%s' "
                    "doesn't support action '%s'" %
                    (service_name, api_version, action_name))
            )

        context = req_context.get_current()

        context.request_type = action_name

        return action.perform(context.tenant, action_params)


@api.with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return pecan.make_app(
        root="magnetodb.api.amz.DynamoDBRootController",
        force_canonical=True,
        guess_content_type_from_ext=False
    )
