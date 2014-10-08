# Copyright 2014 Mirantis Inc.
# Copyright 2014 Symantec Corporation
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
from magnetodb.api import validation
from magnetodb.openstack.common.log import logging

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb.common import probe


LOG = logging.getLogger(__name__)


class ListTablesController():
    """Returns an array of table describing info associated
    with the current user in given tenant.
    """

    @probe.Probe(__name__)
    def list_tables(self, req, project_id):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id

        params = req.params.copy()

        exclusive_start_table_name = params.pop(
            parser.Props.EXCLUSIVE_START_TABLE_NAME, None)
        if exclusive_start_table_name:
            validation.validate_table_name(exclusive_start_table_name)

        limit = params.pop(parser.Props.LIMIT, None)
        if limit:
            limit = validation.validate_integer(limit, parser.Props.LIMIT,
                                                min_val=0)

        validation.validate_unexpected_props(params, "params")

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
