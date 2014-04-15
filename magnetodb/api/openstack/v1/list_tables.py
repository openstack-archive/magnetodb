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

from magnetodb import storage
from magnetodb.openstack.common.log import logging

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils


LOG = logging.getLogger(__name__)


class ListTablesController():
    def list_tables(self, req, project_id):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id
        exclusive_start_table_name = req.params.get(
            parser.Props.EXCLUSIVE_START_TABLE_NAME)

        limit = req.params.get(parser.Props.LIMIT)

        table_names = (
            storage.list_tables(
                req.context,
                exclusive_start_table_name=exclusive_start_table_name,
                limit=limit
            )
        )

        res = {}

        if table_names and str(limit) == str(len(table_names)):
            res[parser.Props.LAST_EVALUATED_TABLE_NAME] = table_names[-1]

        res["tables"] = [{"rel": "self", "href": name} for name in
                         table_names]

        return res
