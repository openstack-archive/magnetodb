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

import collections
import logging

from magnetodb.common import exception
from magnetodb.storage import restore_info_repo
from magnetodb.storage import models

from cassandra import encoder

LOG = logging.getLogger(__name__)


class CassandraRestoreInfoRepository(restore_info_repo.RestoreInfoRepository):
    SYSTEM_TABLE_RESTORE_INFO = 'magnetodb.restore_info'
    __set_field_list = (
        'status', 'backup_id', 'source',
        'start_date_time', 'finish_date_time'
    )

    __get_field_list = (
        'id', 'table_name',
        'status', 'backup_id', 'source',
        'start_date_time', 'finish_date_time'
    )

    def __init__(self, cluster_handler):
        self.__cluster_handler = cluster_handler
        self.__enc = encoder.Encoder()

    def save(self, tenant, restore_job_meta):
        LOG.debug("Saving restore job '{}' for table '{}'".format(
            restore_job_meta.id, restore_job_meta.table_name))

        attrs = {
            name: getattr(restore_job_meta, name)
            for name in self.__set_field_list
        }

        self._update(
            tenant, restore_job_meta.table_name,
            restore_job_meta.id, attrs
        )

        LOG.debug("Saved restore job '{}' for table '{}'".format(
            restore_job_meta.id, restore_job_meta.table_name))
        return restore_job_meta

    def get(self, tenant, table_name, restore_job_id):
        LOG.debug("Getting restore job '{}' for table '{}'".format(
            restore_job_id, table_name))
        query_builder = collections.deque()
        query_builder.append("SELECT * FROM")
        query_builder.append(
            " {} WHERE tenant='{}' AND table_name='{}' AND id={}".format(
                self.SYSTEM_TABLE_RESTORE_INFO,
                tenant, table_name, restore_job_id
            )
        )

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        if not result:
            raise exception.RestoreJobNotExists(
                "Restore job '{}' for table '{}' does not exist".format(
                    restore_job_id, table_name)
            )

        restore_info_attrs = {
            name: result[0].get(name)
            for name in self.__get_field_list
        }

        LOG.debug("Got restore job '{}' for table '{}'".format(
            restore_job_id, table_name))
        return models.RestoreJobMeta(**restore_info_attrs)

    def list(self, tenant, table_name,
             exclusive_start_restore_job_id=None, limit=None):

        query_builder = collections.deque()
        query_builder.append(
            "SELECT * FROM {} WHERE tenant='{}' AND table_name='{}'".format(
                self.SYSTEM_TABLE_RESTORE_INFO,
                tenant, table_name
            )
        )

        if exclusive_start_restore_job_id:
            query_builder.append(
                " AND id > {}".format(
                    self.__enc.cql_encode_all_types(
                        exclusive_start_restore_job_id
                    )
                )
            )

        if limit:
            query_builder.append(" LIMIT {}".format(limit))

        rows = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        LOG.debug('{} restore job(s) selected'.format(len(rows)))

        return [
            models.RestoreJobMeta(
                **{name: row.get(name) for name in self.__get_field_list}
            )
            for row in rows
        ]

    def update(self, tenant, table_name, restore_job_id,
               status=None, finish_date_time=None):

        if status or finish_date_time:

            attrs = {}

            if status:
                attrs['status'] = status

            if finish_date_time:
                attrs['finish_date_time'] = finish_date_time

            self._update(tenant, table_name, restore_job_id, attrs)

        return self.get(tenant, table_name, restore_job_id)

    def _update(self, tenant, table_name, restore_job_id, attrs):
        LOG.debug(
            "Updating attributes {} of restore job '{}' for table '{}'".format(
                attrs, restore_job_id, table_name))

        query_builder = collections.deque()

        query_builder.append(
            "UPDATE {} SET ".format(self.SYSTEM_TABLE_RESTORE_INFO)
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
                tenant, table_name, restore_job_id
            )
        )

        self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        LOG.debug(
            "Updated attributes {} of restore job '{}' for table '{}'".format(
                attrs, restore_job_id, table_name))
