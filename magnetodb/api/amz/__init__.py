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
import webob

from magnetodb.api.amz import dynamodb
from magnetodb.api.amz.dynamodb import exception
from magnetodb.common import wsgi


class AmazonRequest(wsgi.Request):
    default_request_content_types = ('application/x-amz-json-1.0',)
    default_accept_types = ('application/x-amz-json-1.0',)
    default_accept_type = 'application/x-amz-json-1.0'


class AmazonResource(wsgi.Resource):
    def __init__(self, controller, deserializer=None, serializer=None):
        if not deserializer:
            body_deserializers = {
                'application/x-amz-json-1.0': wsgi.JSONDeserializer()
            }
            deserializer = (
                wsgi.RequestDeserializer(body_deserializers=body_deserializers)
            )

        if not serializer:
            body_serializers = {
                'application/x-amz-json-1.0': wsgi.JSONDictSerializer()
            }
            serializer = (
                wsgi.ResponseSerializer(body_serializers=body_serializers)
            )
        super(AmazonResource, self).__init__(controller,
                                             deserializer=deserializer,
                                             serializer=serializer)

    @webob.dec.wsgify(RequestClass=AmazonRequest)
    def __call__(self, request):
        return super(AmazonResource, self).__call__(request)


class AmzDynamoDBApiController():

    capabilities = {
        'DynamoDB': {
            '20111205': dynamodb.capabilities,
            '20120810': dynamodb.capabilities
        }
    }

    def process_action(self, context, service_name, api_version, action_name,
                       action_params):
        service_capabilities = self.capabilities.get(service_name, None)

        if service_capabilities is None:
            raise exception.AWSBadRequestException(
                "Service '%s' isn't supported" % service_name)

        target_capabilities = service_capabilities.get(api_version, None)

        if target_capabilities is None:
            raise (
                exception.AWSBadRequestException(
                    "Service '%s' doesn't support API version '%s'" %
                    (service_name, api_version))
            )

        action = target_capabilities.get(action_name, None)

        if action is None:
            raise (
                exception.AWSValidationException(
                    "Service '%s', API version '%s' "
                    "doesn't support action '%s'" %
                    (service_name, api_version, action_name))
            )

        return action.perform(context, action_params)

    def process_request(self, req, body):
        target = req.environ['HTTP_X_AMZ_TARGET']

        if not target:
            raise (
                exception.AWSValidationException(
                    "'x-amz-target' header wasn't found")
            )

        matcher = re.match("(\w+)_(\w+)\.(\w+)", target)

        if not matcher:
            raise (
                exception.AWSValidationException(
                    "'x-amz-target' header wasn't recognized (actual: %s, "
                    "expected format: <<serviceName>>_<<API version>>."
                    "<<operationName>>")
            )
        service_name = matcher.group(1)
        api_version = matcher.group(2)
        action_name = matcher.group(3)

        return self.process_action(req.context, service_name, api_version,
                                   action_name, body)
