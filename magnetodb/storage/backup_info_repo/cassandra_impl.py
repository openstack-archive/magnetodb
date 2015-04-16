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

import collections
import logging

from magnetodb.common import exception
from magnetodb.storage import backup_info_repo
from magnetodb.storage import models

from cassandra import encoder

LOG = logging.getLogger(__name__)


class CassandraBackupInfoRepository(backup_info_repo.BackupInfoRepository):
    SYSTEM_TABLE_BACKUP_INFO = 'magnetodb.backup_info'
    __set_field_list = (
        'name', 'status', 'start_date_time',
        'finish_date_time', 'location', 'strategy'
    )

    __get_field_list = (
        'id', 'name', 'table_name', 'status', 'start_date_time',
        'finish_date_time', 'location', 'strategy'
    )

    def __init__(self, cluster_handler):
        self.__cluster_handler = cluster_handler
        self.__enc = encoder.Encoder()

    def delete(self, tenant, table_name, backup_id):
        LOG.debug("Deleting backup '{}' for table '{}'".format(
            backup_id, table_name))
        backup_meta = self.get(tenant, table_name, backup_id)

        query_builder = collections.deque()
        query_builder.append("DELETE FROM")
        query_builder.append(
            " {} WHERE tenant='{}' AND table_name='{}' AND id={}".format(
                self.SYSTEM_TABLE_BACKUP_INFO,
                tenant, table_name, backup_id
            )
        )

        self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        LOG.debug("Deleted backup '{}' for table '{}'".format(
            backup_id, table_name))
        return backup_meta

    def save(self, tenant, backup_meta):
        LOG.debug("Saving backup '{}' for table '{}'".format(
            backup_meta.id, backup_meta.table_name))

        attrs = {
            name: getattr(backup_meta, name)
            for name in self.__set_field_list
        }

        self._update(tenant, backup_meta.table_name, backup_meta.id, attrs)

        LOG.debug("Saved backup '{}' for table '{}'".format(
            backup_meta.id, backup_meta.table_name))
        return backup_meta

    def get(self, tenant, table_name, backup_id):
        LOG.debug("Getting backup '{}' for table '{}'".format(
            backup_id, table_name))
        query_builder = collections.deque()
        query_builder.append("SELECT * FROM")
        query_builder.append(
            " {} WHERE tenant='{}' AND table_name='{}' AND id={}".format(
                self.SYSTEM_TABLE_BACKUP_INFO,
                tenant, table_name, backup_id
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

        LOG.debug("Got backup '{}' for table '{}'".format(
            backup_id, table_name))
        return models.BackupMeta(**backup_info_attrs)

    def list(self, tenant, table_name,
             exclusive_start_backup_id=None, limit=None):

        query_builder = collections.deque()
        query_builder.append(
            "SELECT * FROM {} WHERE tenant='{}' AND table_name='{}'".format(
                self.SYSTEM_TABLE_BACKUP_INFO,
                tenant, table_name
            )
        )

        if exclusive_start_backup_id:
            query_builder.append(
                " AND id > {}".format(
                    self.__enc.cql_encode_all_types(
                        exclusive_start_backup_id
                    )
                )
            )

        if limit:
            query_builder.append(" LIMIT {}".format(limit))

        rows = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        return [
            models.BackupMeta(
                **{name: row[name] for name in self.__get_field_list}
            )
            for row in rows
        ]

    def update(self, tenant, table_name, backup_id,
               status=None, finish_date_time=None, location=None):

        if status or finish_date_time or location:

            attrs = {}

            if status:
                attrs['status'] = status

            if finish_date_time:
                attrs['finish_date_time'] = finish_date_time

            if location:
                attrs['location'] = location

            self._update(tenant, table_name, backup_id, attrs)

        return self.get(tenant, table_name, backup_id)

    def _update(self, tenant, table_name, backup_id, attrs):
        LOG.debug(
            "Updating attributes {} of backup '{}' for table '{}'".format(
                attrs, backup_id, table_name))

        query_builder = collections.deque()

        query_builder.append(
            "UPDATE {} SET ".format(self.SYSTEM_TABLE_BACKUP_INFO)
        )

        query_builder.append(
            ",".join(
                [
                    '"{}"={}'.format(
                        name,
                        self.__enc.cql_encode_all_types(value))
                    for name, value in attrs.iteritems()
                    if name in self.__set_field_list
                ]
            )
        )

        query_builder.append(
            " WHERE tenant='{}' AND table_name='{}' AND id={}".format(
                tenant, table_name, backup_id
            )
        )

        self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        LOG.debug(
            "Updated attributes {} of backup '{}' for table '{}'".format(
                attrs, backup_id, table_name))
