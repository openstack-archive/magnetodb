# Copyright 2015 Mirantis Inc.
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

from magnetodb.api.openstack.v1 import parser
from magnetodb.api import validation
from magnetodb.openstack.common import log as logging
from magnetodb import storage

LOG = logging.getLogger(__name__)


class AllProjectsUsageController():
    """Returns metrics.
    """

    def projects_usage_details(self, req):
        allowed_keys = ['size', 'item_count']
        if 'metrics' not in req.GET:
            keys = allowed_keys
        else:
            keys = req.GET['metrics'].split(',')
            validation.validate_metrics(keys, allowed_keys)

        limit = req.GET.get('limit', 1000)
        limit = validation.validate_integer(limit, parser.Props.LIMIT,
                                            min_val=0)
        last_evaluated_table = req.GET.get('last_evaluated_table')
        if last_evaluated_table:
            validation.validate_table_name(last_evaluated_table)

        last_evaluated_project = req.GET.get('last_evaluated_project'),
        if last_evaluated_project:
            validation.validate_project_id(last_evaluated_project)

        tables = storage.list_tenant_tables(
            last_evaluated_project=last_evaluated_project,
            last_evaluated_table=last_evaluated_table
        )

        success_count = 0
        result = []
        for row in tables:
            req.context.tenant = row["tenant"]
            row["usage_details"] = storage.get_table_statistics(
                req.context, row["name"], keys
            )
            if row["usage_details"]:
                result.append(row)
                success_count += 1
            if success_count == limit:
                break

        return result
