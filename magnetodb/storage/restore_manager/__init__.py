# Copyright 2015 Mirantis Inc.
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

from oslo_utils import timeutils
import uuid

from magnetodb.storage import models


class RestoreManager(object):

    def __init__(self, restore_info_repo):
        self.restore_info_repo = restore_info_repo

    def create_restore_job(self, tenant, table_name, backup_id, source):
        """Create restore job

        :param tenant: tenant for table
        :param table_name: String, name of the table to restore
        :param backup_id: String, id of the backup to restore from
        :param source: String, source of the backup to restore from

        :returns: RestoreJobMeta

        :raises: BackendInteractionException
        """

        restore_meta = models.RestoreJobMeta(
            id=uuid.uuid4(),
            table_name=table_name,
            status=models.RestoreJobMeta.RESTORE_STATUS_RESTORING,
            backup_id=backup_id,
            source=source,
            start_date_time=timeutils.utcnow())

        return self.restore_info_repo.save(tenant, restore_meta)

    def describe_restore_job(self, tenant, table_name, restore_job_id):
        return self.restore_info_repo.get(tenant, table_name, restore_job_id)

    def list_restore_jobs(self, tenant, table_name,
                          exclusive_start_restore_job_id, limit):

        return self.restore_info_repo.list(
            tenant, table_name, exclusive_start_restore_job_id, limit)
