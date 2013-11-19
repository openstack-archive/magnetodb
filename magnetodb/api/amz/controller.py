# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 OpenStack LLC.
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

from magnetodb.common import exception
from magnetodb.api.amz import manager


class AmzDynamoDBApiController():

    capabilities = {
        'DynamoDB': {
            '20111205': manager.DynamoDBManager()
        }
    }

    def do_process_request(self, service_name, api_version, operation_name,
                           param):
        service_capabilities = self.capabilities.get(service_name, None)

        if service_capabilities is None:
            raise exception.ServiceUnavailableException(
                "Service '%s' isn't supported" % service_name)

        target_manager = service_capabilities.get(api_version, None)

        if target_manager is None:
            raise (
                exception.ServiceUnavailableException(
                    "Service '%s' doesn't support API version '%s'" %
                    (service_name, api_version))
            )

        operation_name = re.sub("(.)([A-Z])", r"\1_\2", operation_name).lower()

        operation = getattr(target_manager, operation_name, None)

        if operation is None:
            raise (
                exception.ValidationException(
                    "Service '%s', API version '%s' "
                    "doesn't support operation '%s'" %
                    (service_name, api_version, operation_name))
            )

        return operation(param) if param else operation()

    def process_request(self, req, body):
        target = req.environ['HTTP_X_AMZ_TARGET']

        if not target:
            raise (
                exception.ValidationException(
                    "'x-amz-target' header wasn't found")
            )

        matcher = re.match("(\w+)_(\w+)\.(\w+)", target)

        if not matcher:
            raise (
                exception.ValidationException(
                    "'x-amz-target' header wasn't recognized (actual: %s, "
                    "expected format: <<serviceName>>_<<API version>>."
                    "<<operationName>>")
            )
        service_name = matcher.group(1)
        api_version = matcher.group(2)
        operation_name = matcher.group(3)

        self.do_process_request(service_name, api_version, operation_name,
                                body)
