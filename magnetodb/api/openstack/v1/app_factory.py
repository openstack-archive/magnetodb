# Copyright 2014 Symantec Corporation
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

from magnetodb.api import with_global_env
from magnetodb.common import wsgi

from magnetodb.api.openstack.v1 import create_resource
from magnetodb.api.openstack.v1.request import get_item
from magnetodb.api.openstack.v1.request import batch_get_item
from magnetodb.api.openstack.v1.request import batch_write_item
from magnetodb.api.openstack.v1.request import delete_item
from magnetodb.api.openstack.v1.request import put_item
from magnetodb.api.openstack.v1.request import update_item
from magnetodb.api.openstack.v1.request import list_tables
from magnetodb.api.openstack.v1.request import create_table
from magnetodb.api.openstack.v1.request import describe_table
from magnetodb.api.openstack.v1.request import scan
from magnetodb.api.openstack.v1.request import query
from magnetodb.api.openstack.v1.request import delete_table


class MagnetoDBApplication(wsgi.Router):

    """API"""
    def __init__(self):
        mapper = routes.Mapper()
        super(MagnetoDBApplication, self).__init__(mapper)

        mapper.connect(
            "batch_write_item", "/{project_id}/data/batch_write_item",
            conditions={'method': 'POST'},
            controller=create_resource(
                batch_write_item.BatchWriteItemController()),
            action="process_request"
        ),
        mapper.connect(
            "put_item", "/{project_id}/data/tables/{table_name}/put_item",
            conditions={'method': 'POST'},
            controller=create_resource(put_item.PutItemController()),
            action="process_request"
        ),
        mapper.connect(
            "get_item", "/{project_id}/data/tables/{table_name}/get_item",
            conditions={'method': 'POST'},
            controller=create_resource(get_item.GetItemController()),
            action="process_request"
        ),
        mapper.connect(
            "delete_item",
            "/{project_id}/data/tables/{table_name}/delete_item",
            conditions={'method': 'POST'},
            controller=create_resource(delete_item.DeleteItemController()),
            action="process_request"
        ),
        mapper.connect(
            "update_item",
            "/{project_id}/data/tables/{table_name}/update_item",
            conditions={'method': 'POST'},
            controller=create_resource(update_item.UpdateItemController()),
            action="process_request"
        ),
        mapper.connect(
            "batch_get_item", "/{project_id}/data/batch_get_item",
            conditions={'method': 'POST'},
            controller=create_resource(
                batch_get_item.BatchGetItemController()),
            action="process_request"
        ),
        mapper.connect(
            "list_tables", "/{project_id}/data/tables",
            conditions={'method': 'GET'},
            controller=create_resource(list_tables.ListTablesController()),
            action="list_tables"
        ),
        mapper.connect(
            "create_table", "/{project_id}/data/tables",
            conditions={'method': 'POST'},
            controller=create_resource(create_table.CreateTableController()),
            action="create_table"
        ),
        mapper.connect(
            "describe_table", "/{project_id}/data/tables/{table_name}",
            conditions={'method': 'GET'},
            controller=create_resource(
                describe_table.DescribeTableController()),
            action="describe_table"
        ),
        mapper.connect(
            "scan", "/{project_id}/data/tables/{table_name}/scan",
            conditions={'method': 'POST'},
            controller=create_resource(scan.ScanController()),
            action="scan"
        ),
        mapper.connect(
            "query", "/{project_id}/data/tables/{table_name}/query",
            conditions={'method': 'POST'},
            controller=create_resource(query.QueryController()),
            action="query"
        ),
        mapper.connect(
            "delete_table", "/{project_id}/data/tables/{table_name}",
            conditions={'method': 'DELETE'},
            controller=create_resource(delete_table.DeleteTableController()),
            action="delete_table"
        )


@with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return MagnetoDBApplication()
