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

from magnetodb.api.rest.action import list_tables
from magnetodb.api.rest.action import create_table

from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)


class RestApiController():

    def list_tables(self, req, project_id=None):
        LOG.debug('context:' + str(req.context.to_dict()))
        LOG.debug('path_url:' + req.path_url)

        req.context.tenant = project_id
        return list_tables.ListTablesRestAction.perform(
            req.context, dict(req.params))

    def create_table(self, req, body, project_id=None):
        req.context.tenant = project_id
        req.context.path_url = req.path_url
        return create_table.CreateTableRestAction.perform(
            req.context, body)

    def process_table(self, req, body=None,
                      project_id=None, table_name=None):
        return {"not implemented yet": 1}

    def process_items(self, req, body=None,
                      project_id=None, table_name=None,
                      item_action=None):
        return {"not implemented yet": 1}