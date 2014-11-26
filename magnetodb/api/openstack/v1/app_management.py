# Copyright 2013 Mirantis Inc.
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

from magnetodb.api import with_global_env
from magnetodb.common import wsgi

from magnetodb.api.openstack.v1 import create_resource

from magnetodb.api.openstack.v1.management import create_backup
from magnetodb.api.openstack.v1.management import delete_backup
from magnetodb.api.openstack.v1.management import describe_backup
from magnetodb.api.openstack.v1.management import list_backups
from magnetodb.api.openstack.v1.management import create_restore_job
from magnetodb.api.openstack.v1.management import describe_restore_job
from magnetodb.api.openstack.v1.management import list_restore_jobs


class ManagementApplication(wsgi.Router):

    """API"""
    def __init__(self):
        mapper = routes.Mapper()
        super(ManagementApplication, self).__init__(mapper)

        mapper.connect(
            "create_backup",
            "/management/{project_id}/{table_id}/backups",
            conditions={'method': 'POST'},
            controller=create_resource(
                create_backup.CreateBackupController()),
            action="create_table"
        )

        mapper.connect(
            "list_backups",
            "/management/{project_id}/{table_id}/backups",
            conditions={'method': 'GET'},
            controller=create_resource(
                list_backups.ListBackupsController()),
            action="list_backups"
        )

        mapper.connect(
            "describe_backup",
            "/management/{project_id}/{table_id}/backups/{backup_id}",
            conditions={'method': 'GET'},
            controller=create_resource(
                describe_backup.DescribeBackupController()),
            action="describe_backup"
        )

        mapper.connect(
            "delete_backup",
            "/management/{project_id}/{table_id}/backups/{backup_id}",
            conditions={'method': 'DELETE'},
            controller=create_resource(
                delete_backup.DeleteBackupController()),
            action="delete_backup"
        )

        mapper.connect(
            "create_restore_job",
            "/management/{project_id}/{table_id}/restores",
            conditions={'method': 'POST'},
            controller=create_resource(
                create_restore_job.CreateRestoreJobController()),
            action="create_restore_job"
        )

        mapper.connect(
            "list_restore_jobs",
            "/management/{project_id}/{table_id}/restores",
            conditions={'method': 'GET'},
            controller=create_resource(
                list_restore_jobs.ListRestoreJobsController()),
            action="list_restore_jobs"
        )

        mapper.connect(
            "describe_restore_job",
            "/management/{project_id}/{table_id}/restores/{restore_job_id}",
            conditions={'method': 'GET'},
            controller=create_resource(
                describe_restore_job.DescribeRestoreJobController()),
            action="describe_restore_job"
        )


@with_global_env(default_program='management-api')
def app_factory(global_conf, **local_conf):
    return ManagementApplication()
