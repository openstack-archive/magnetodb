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
from magnetodb.openstack.common.log import logging


LOG = logging.getLogger(__name__)


class TableUsageController():
    """Returns metrics.
    """

    def table_usage_details(self, req, project_id, table_name):
        req.context.tenant = project_id

        if 'metrics' not in req.GET:
            keys = ['size', 'item_count']
        else:
            keys = req.GET['metrics'].split(',')

        result = storage.get_table_statistics(
            req.context, table_name, keys)

        return result
