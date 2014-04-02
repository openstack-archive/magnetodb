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

from routes.base import Route

from magnetodb.common import wsgi
from magnetodb.openstack.common.log import logging

from magnetodb.api.openstack.v1 import put_item
from magnetodb.api.openstack.v1 import get_item
from magnetodb.api.openstack.v1 import batch_write_item
from magnetodb.api.openstack.v1 import delete_item


LOG = logging.getLogger(__name__)


def create_resource(controller, options=None):
    body_deserializers = {
        'application/json': wsgi.JSONDeserializer()
    }
    deserializer = wsgi.RequestDeserializer(
        body_deserializers=body_deserializers)

    body_serializers = {
        'application/json': wsgi.JSONDictSerializer()
    }
    serializer = wsgi.ResponseSerializer(body_serializers=body_serializers)

    return wsgi.Resource(controller, deserializer, serializer)


openstack_api = [
    Route("batch_write_item", "/{project_id}/data/batch_write_item",
          conditions={'method': 'POST'},
          controller=create_resource(
              batch_write_item.BatchWriteItemController()),
          action="process_request"),

    Route("put_item", "/{project_id}/data/tables/{table_name}/put_item",
          conditions={'method': 'POST'},
          controller=create_resource(put_item.PutItemController()),
          action="process_request"),

    Route("get_item", "/{project_id}/data/tables/{table_name}/get_item",
          conditions={'method': 'POST'},
          controller=create_resource(get_item.GetItemController()),
          action="process_request"),

    Route("delete_item", "/{project_id}/data/tables/{table_name}/delete_item",
          conditions={'method': 'POST'},
          controller=create_resource(delete_item.DeleteItemController()),
          action="process_request"),
]
