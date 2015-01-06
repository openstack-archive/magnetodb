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
from magnetodb.api.openstack.v1.monitoring import project_usage_details

LOG = logging.getLogger(__name__)


class AllProjectsUsageController():
    """Returns metrics.
    """

    def projects_usage_details(self, req):
        result = []
        for tenant in storage.list_tenants():
            req.context.tenant = tenant
            rq_handler = project_usage_details.ProjectUsageController()
            result.append({
                "project_id": tenant,
                "tables": rq_handler.project_usage_details(req, tenant)
            })

        return result
