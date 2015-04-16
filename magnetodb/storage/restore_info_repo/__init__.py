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

from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class RestoreInfoRepository(object):

    def get(self, tenant, table_name, restore_job_id):
        """Get restore job info

        :param tenant: tenant for table
        :param table_name: string, name of the restored table
        :param restore_job_id: string, id of the restore job

        :returns: RestoreJobMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def list(self, tenant, table_name,
             exclusive_start_restore_job_id=None,
             limit=None):
        """List restore job info items for a table

        :param tenant: tenant for table
        :param table_name: string, name of the restored table
        :param exclusive_start_restore_job_id: string, last restore job id,
                retrieved in previous list call
        :param limit:int, limit of returned restore job info items

        :returns: list of RestoreJobMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def save(self, tenant, restore_job_meta):
        """Save restore job info

        :param tenant: tenant for table
        :param restore_job_meta: RestoreJobMeta, restore job info to save

        :returns: RestoreJobMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def update(self, tenant, table_name, restore_job_id,
               status=None, finish_date_time=None):
        """Update editable attributes of restore job info

        :param table_name: string, name of the restored table
        :param restore_job_id: string, id of the restore job
        :param status: string, new value for status
                or None to keep it as is
        :param finish_date_time: datetime, new value for finish_date_time
                or None to keep it as is

        :returns: updated RestoreJobMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()
