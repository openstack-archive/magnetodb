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

import routes

from magnetodb import api
from magnetodb.common import wsgi
from magnetodb.api.openstack import v1 as api_v1
from magnetodb.api.openstack.v1.monitoring import table_usage_details
from magnetodb.api.openstack.v1.monitoring import project_usage_details
from magnetodb.api.openstack.v1.monitoring import all_projects_usage_details


class MonitoringApplication(wsgi.Router):

    """Monitoring API"""

    def __init__(self):
        mapper = routes.Mapper()
        super(MonitoringApplication, self).__init__(mapper)

        mapper.connect(
            "monitor_table",
            "/projects/{project_id}/tables/{table_name}",
            controller=api_v1.create_resource(
                table_usage_details.TableUsageController()),
            conditions={'method': 'GET'},
            action="table_usage_details"
        )
        mapper.connect(
            "monitor_project",
            "/projects/{project_id}",
            controller=api_v1.create_resource(
                project_usage_details.ProjectUsageController()),
            conditions={'method': 'GET'},
            action="project_usage_details"
        )
        mapper.connect(
            "monitor_all_projects",
            "/projects",
            controller=api_v1.create_resource(
                all_projects_usage_details.AllProjectsUsageController()),
            conditions={'method': 'GET'},
            action="projects_usage_details"
        )


@api.with_global_env(default_program='monitoring-api')
def app_factory(global_conf, **local_conf):
    return MonitoringApplication()
