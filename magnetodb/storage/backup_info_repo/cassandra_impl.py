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
import collections
import logging

from cassandra import encoder

from magnetodb.common import exception

from magnetodb.storage.backup_info_repo import BackupInfo
from magnetodb.storage.backup_info_repo import BackupInfoRepository

LOG = logging.getLogger(__name__)


class CassandraBackupInfoRepository(BackupInfoRepository):
    SYSTEM_TABLE_BACKUP_INFO = 'magnetodb.backup_info'
    __set_field_list = (
        'name', 'table_name', 'status', 'start_date_time',
        'finish_date_time', 'location'
    )

    __get_field_list = (
        'id', 'name', 'table_name', 'status', 'start_date_time',
        'finish_date_time', 'location'
    )

    def __init__(self, cluster_handler):
        self.__cluster_handler = cluster_handler
        self.__enc = encoder.Encoder()

    def delete(self, context, table_name, backup_id):
        LOG.debug("Deleting backup '{}' for table '{}'".format(
            backup_id, table_name))
        backup_info = self.get(context, table_name, backup_id)
        backup_info.status = BackupInfo.BACKUP_STATUS_DELETING
        self.save(context, table_name, backup_info)
        LOG.debug("Deleted backup '{}' for table '{}'".format(
            backup_id, table_name))
        return backup_info

    def save(self, context, table_name, backup_info):
        LOG.debug("Saving backup '{}' for table '{}'".format(
            backup_info.id, table_name))
        backup_info.table_name = table_name

        query_builder = collections.deque()

        query_builder.append(
            "UPDATE {} SET ".format(self.SYSTEM_TABLE_BACKUP_INFO)
        )

        query_builder.append(
            ",".join(
                [
                    '"{}"={}'.format(
                        field,
                        self.__enc.cql_encode_all_types(
                            getattr(backup_info, field)))
                    for field in self.__set_field_list
                ]
            )
        )

        query_builder.append(
            " WHERE tenant_table='{}' AND id='{}'".format(
                self.__backup_hash(context, table_name),
                backup_info.id
            )
        )

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        if not result[0]['[applied]']:
            raise exception.BackendInteractionException(
                "Backup info '{}' for table '{}' was not saved".format(
                    backup_info.id, table_name)
            )

        LOG.debug("Saved backup '{}' for table '{}'".format(
            backup_info.id, table_name))
        return backup_info

    @staticmethod
    def __backup_hash(context, table_name):
        return context.tenant + ':' + table_name

    def get(self, context, table_name, backup_id):
        LOG.debug("Getting backup '{}' for table '{}'".format(
            backup_id, table_name))
        query_builder = collections.deque()
        query_builder.append("SELECT *")
        query_builder.append(
            " FROM {} WHERE tenant_table='{}' AND id='{}'".format(
                self.SYSTEM_TABLE_BACKUP_INFO,
                self.__backup_hash(context, table_name), backup_id
            )
        )

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        if not result:
            raise exception.BackupNotExists(
                "Backup '{}' for table '{}' does not exist".format(
                    backup_id, table_name)
            )

        backup_info_attrs = {
            name: result[0].get(name)
            for name in self.__get_field_list
        }

        LOG.debug("Get backup '{}' for table '{}'".format(
            backup_id, table_name))
        return BackupInfo(**backup_info_attrs)

    def list(self, context, table_name,
             exclusive_start_backup_id=None, limit=None):
        return []
