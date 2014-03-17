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

from magnetodb.api.openstack.v1 import put_item


openstack_api = [
    Route("put_item", "/{project_id}/data/tables/{table_name}/put_item",
          conditions={'method': 'POST'},
          controller=put_item.create_resource(None),
          action="process_request"),
]

