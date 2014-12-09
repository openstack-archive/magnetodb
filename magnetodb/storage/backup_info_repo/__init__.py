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

from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class BackupInfo(object):
    BACKUP_STATUS_CREATING = "CREATING"
    BACKUP_STATUS_DELETING = "DELETING"
    BACKUP_STATUS_CREATED = "CREATED"
    BACKUP_STATUS_CREATE_FAILED = "CREATE_FAILED"
    BACKUP_STATUS_DELETE_FAILED = "DELETE_FAILED"

    _allowed_statuses = set([BACKUP_STATUS_CREATING,
                             BACKUP_STATUS_DELETING,
                             BACKUP_STATUS_CREATED,
                             BACKUP_STATUS_CREATE_FAILED,
                             BACKUP_STATUS_DELETE_FAILED])

    def __init__(self, id, name, table_name, status, location,
                 start_date_time=None, finish_date_time=None, strategy=None):

        assert status in self._allowed_statuses, (
            "Backup status '%s' is not allowed" % status
        )

        self.id = id
        self.name = name
        self.table_name = table_name
        self.status = status
        self.start_date_time = start_date_time
        self.finish_date_time = finish_date_time
        self.location = location
        self.strategy = strategy


class BackupInfoRepository(object):

    def get(self, context, table_name, backup_id):
        raise NotImplementedError()

    def list(self, context, table_name,
             exclusive_start_backup_id=None,
             limit=None):
        raise NotImplementedError()

    def save(self, context, table_name, backup_info):
        """
        Save backup info

        :param context: current request context
        :param table_name: String, name of the table to backup
        :param backup_info: BackupInfo, backup info to save

        :returns:

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def delete(self, context, table_name, backup_id):
        raise NotImplementedError()
