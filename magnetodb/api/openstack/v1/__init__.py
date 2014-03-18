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
from magnetodb.api.openstack.v1 import create_table
from magnetodb.api.openstack.v1 import list_tables

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


# TODO(achudnovets): use single controller for tables
openstack_api = [
    Route("create_table", "/{project_id}/data/tables",
          conditions={'method': 'POST'},
          controller=create_resource(create_table.CreateTableController),
          action="create_table"),
    Route("list_tables", "/{project_id}/data/tables",
          conditions={'method': 'GET'},
          controller=create_resource(list_tables.ListTablesController()),
          action="create_table"),
    Route("put_item", "/{project_id}/data/tables/{table_name}/put_item",
          conditions={'method': 'POST'},
          controller=create_resource(put_item.PutItemController()),
          action="process_request"),
]
