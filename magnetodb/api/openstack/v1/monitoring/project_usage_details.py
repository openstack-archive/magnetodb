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

from magnetodb.openstack.common import log as logging
from magnetodb import storage
from magnetodb.common import exception

LOG = logging.getLogger(__name__)


class ProjectUsageController():
    """Returns metrics.
    """

    def project_usage_details(self, req, project_id):
        req.context.tenant = project_id

        if 'metrics' not in req.GET:
            keys = ['size', 'item_count']
        else:
            keys = req.GET['metrics'].split(',')

        table_names = storage.list_tables(req.context)

        result = []
        for table_name in table_names:
            try:
                result.append({
                    "table_name": table_name,
                    "usage_details": storage.get_table_statistics(req.context,
                                                                  table_name,
                                                                  keys)
                })
            except (exception.ValidationError,
                    exception.TableNotExistsException):
                pass

        return result
