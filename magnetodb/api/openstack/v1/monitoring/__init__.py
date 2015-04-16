from magnetodb import api

from magnetodb.api.openstack.v1.monitoring import project_usage_details
from magnetodb.api.openstack.v1.monitoring import all_projects_usage_details
from magnetodb.api.openstack.v1.monitoring import table_usage_details

import pecan

from pecan import rest


class TablesController(rest.RestController):
    @pecan.expose("json")
    def get_one(self, project_id, table_name):
        return table_usage_details.table_usage_details(
            pecan.request, project_id, table_name
        )


class MonitoringRootController(rest.RestController):

    """API"""

    tables = TablesController()

    @pecan.expose("json")
    def get_one(self, project_id):
        return project_usage_details.project_usage_details(
            pecan.request, project_id
        )

    @pecan.expose("json")
    def get_all(self):
        return all_projects_usage_details.all_projects_usage_details(
            pecan.request
        )


@api.with_global_env(default_program='management-api')
def app_factory(global_conf, **local_conf):
    return pecan.make_app(
        root="magnetodb.api.openstack.v1.monitoring.MonitoringRootController",
        force_canonical=True,
        guess_content_type_from_ext=False
    )
