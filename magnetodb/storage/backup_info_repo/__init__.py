# Copyright 2014 Mirantis Inc.
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


class BackupInfoRepository(object):

    def get(self, tenant, table_name, backup_id):
        """Get backup info

        :param tenant: tenant for table
        :param table_name: string, name of the backed up table
        :param backup_id: string, id of the backup

        :returns: BackupMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def list(self, tenant, table_name,
             exclusive_start_backup_id=None,
             limit=None):
        """List backup info items for a table

        :param tenant: tenant for table
        :param table_name: string, name of the backed up table
        :param exclusive_start_backup_id: string, last backup id,
                retrieved in previous list call
        :param limit:int, limit of returned backup info items

        :returns: list of BackupMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def save(self, tenant, backup_meta):
        """Save backup info

        :param tenant: tenant for table
        :param backup_meta: BackupMeta, backup info to save

        :returns: BackupMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def delete(self, tenant, table_name, backup_id):
        """Delete backup info

        :param tenant: tenant for table
        :param table_name: string, name of the backed up table
        :param backup_id: string, id of the backup

        :returns: BackupMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def update(self, tenant, table_name, backup_id,
               status=None, finish_date_time=None, location=None):
        """Update editable attributes of backup info

        :param tenant: tenant for table
        :param table_name: string, name of the backed up table
        :param backup_id: string, id of the backup
        :param status: string, new value for status
                or None to keep it as is
        :param finish_date_time: datetime, new value for finish_date_time
                or None to keep it as is
        :param location: string, new value for location
                or None to keep it as is

        :returns: updated BackupMeta

        :raises: BackendInteractionException
        """
        raise NotImplementedError()
