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

from magnetodb.common import wsgi


class PutItemController(object):
    def process_request(self, req, body, project_id, table_name):
        return {'Hello': 'world'}


def create_resource(options):
    body_deserializers = {
        'application/json': wsgi.JSONDeserializer()
    }
    deserializer = wsgi.RequestDeserializer(
        body_deserializers=body_deserializers)

    body_serializers = {
        'application/json': wsgi.JSONDictSerializer()
    }
    serializer = wsgi.ResponseSerializer(body_serializers=body_serializers)

    return wsgi.Resource(PutItemController(), deserializer, serializer)
