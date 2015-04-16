from magnetodb import api

from magnetodb.api.openstack.v1.management import create_backup
from magnetodb.api.openstack.v1.management import delete_backup
from magnetodb.api.openstack.v1.management import describe_backup
from magnetodb.api.openstack.v1.management import list_backups
from magnetodb.api.openstack.v1.management import create_restore_job
from magnetodb.api.openstack.v1.management import describe_restore_job
from magnetodb.api.openstack.v1.management import list_restore_jobs

import pecan
from pecan import rest


class BackupsController(rest.RestController):
    @pecan.expose("json")
    def delete(self, project_id, table_name, backup_id):
        return delete_backup.delete_backup(
            pecan.request, project_id, table_name, backup_id
        )

    @pecan.expose("json")
    def post(self, project_id, table_name):
        return create_backup.create_backup(pecan.request, project_id,
                                           table_name)

    @pecan.expose("json")
    def get_all(self, project_id, table_name):
        return list_backups.list_backups(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def get_one(self, project_id, table_name, backup_id):
        return describe_backup.describe_backup(
            pecan.request, project_id, table_name, backup_id
        )


class RestoresController(rest.RestController):
    @pecan.expose("json")
    def post(self, project_id, table_name):
        return create_restore_job.create_restore_job(
            pecan.request, project_id, table_name
        )

    @pecan.expose("json")
    def get_all(self, project_id, table_name):
        return list_restore_jobs.list_restore_jobs(
            pecan.request, project_id, table_name
        )

    @pecan.expose("json")
    def get_one(self, project_id, table_name, restore_job_id):
        return describe_restore_job.describe_restore_job(
            pecan.request, project_id, table_name, restore_job_id
        )


class TablesController(rest.RestController):
    backups = BackupsController()
    restores = RestoresController()

    @pecan.expose("json")
    def get_one(self, project_id, table_name):
        pecan.abort(404)


class ManagementRootController(rest.RestController):

    """API"""

    tables = TablesController()

    @pecan.expose("json")
    def get_one(self, project_id):
        pecan.abort(404)


@api.with_global_env(default_program='management-api')
def app_factory(global_conf, **local_conf):
    return pecan.make_app(
        root="magnetodb.api.openstack.v1.management.ManagementRootController",
        force_canonical=True,
        guess_content_type_from_ext=False
    )
