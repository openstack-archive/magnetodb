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
import webob


class RestRequest(wsgi.Request):
    default_request_content_types = ('application/x-magnetodb-json-1.0',)
    default_accept_types = ('application/x-magnetodb-json-1.0',)
    default_accept_type = 'application/x-magnetodb-json-1.0'


class RestResource(wsgi.Resource):
    def __init__(self, controller, deserializer=None, serializer=None):
        if not deserializer:
            body_deserializers = {
                'application/x-magnetodb-json-1.0': wsgi.JSONDeserializer()
            }
            deserializer = (
                wsgi.RequestDeserializer(body_deserializers=body_deserializers)
            )

        if not serializer:
            body_serializers = {
                'application/x-magnetodb-json-1.0': wsgi.JSONDictSerializer()
            }
            serializer = (
                wsgi.ResponseSerializer(body_serializers=body_serializers)
            )
        super(RestResource, self).__init__(controller,
                                             deserializer=deserializer,
                                             serializer=serializer)

    @webob.dec.wsgify(RequestClass=RestRequest)
    def __call__(self, request):
        return super(RestResource, self).__call__(request)
