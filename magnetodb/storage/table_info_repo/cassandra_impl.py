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
from threading import Lock

from magnetodb.common import exception
from magnetodb.storage import models
from magnetodb.storage.table_info_repo import TableInfo


class CassandraTableInfoRepository(object):
    SYSTEM_TABLE_TABLE_INFO = 'magnetodb.table_info'
    __field_list = ("schema", "internal_name", "status")

    def _save_table_info_to_cache(self, context, table_info):
        tenant_tables_cache = self.__table_info_cache.get(context.tenant)
        if tenant_tables_cache is None:
            tenant_tables_cache = {}
            self.__table_info_cache[context.tenant] = tenant_tables_cache

        # TODO(dukhlov):
        # It is temporary solution to set internal_keyspace
        # In future keyspace should be saved in metadata too
        from magnetodb.storage.driver.cassandra import USER_PREFIX
        table_info.internal_keyspace = USER_PREFIX + context.tenant

        tenant_tables_cache[table_info.name] = table_info

    def _get_table_info_from_cache(self, context, table_name):
        tenant_tables_cache = self.__table_info_cache.get(context.tenant)
        if tenant_tables_cache is None:
            return None
        return tenant_tables_cache.get(table_name)

    def _remove_table_schema_from_cache(self, context, table_name):
        tenant_tables_cache = self.__table_info_cache.get(context.tenant)
        if tenant_tables_cache is None:
            return None

        return tenant_tables_cache.pop(table_name, None)

    def __init__(self, cluster_handler):
        self.__cluster_handler = cluster_handler
        self.__table_info_cache = {}
        self.__table_cache_lock = Lock()

    def get(self, context, table_name, refresh_field_list=None):
        table_info = self._get_table_info_from_cache(context, table_name)
        if table_info:
            if refresh_field_list:
                self.refresh(context, table_info, refresh_field_list)
            return table_info

        with self.__table_cache_lock:
            table_info = self._get_table_info_from_cache(context, table_name)
            if table_info:
                return table_info

            table_info = TableInfo(table_name)
            self.refresh(context, table_info)
            self._save_table_info_to_cache(context, table_info)
        return table_info

    def get_tenant_table_names(self, context, exclusive_start_table_name=None,
                               limit=None):
        query_builder = collections.deque()
        query_builder.append(
            "SELECT name FROM {} WHERE tenant='{}'".format(
                self.SYSTEM_TABLE_TABLE_INFO, context.tenant
            )
        )

        if exclusive_start_table_name:
            query_builder.append(
                " AND name > '{}'".format(exclusive_start_table_name)
            )

        if limit:
            query_builder.append(" LIMIT {}".format(limit))

        tables = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        return [row['name'] for row in tables]

    def refresh(self, context, table_info, field_list=__field_list):
        query_builder = collections.deque()
        query_builder.append("SELECT ")
        query_builder.append(",".join(map('"{}"'.format, field_list)))

        query_builder.append(
            " FROM {} WHERE tenant='{}' AND name='{}'".format(
                self.SYSTEM_TABLE_TABLE_INFO,
                context.tenant, table_info.name
            )
        )

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        if not result:
            self._remove_table_schema_from_cache(context, table_info.name)
            raise exception.TableNotExistsException(
                "Table '{}' does not exist".format(table_info.name)
            )
        for name, value in result[0].iteritems():
            if name == "schema":
                value = models.ModelBase.from_json(value)
            setattr(table_info, name, value)
        return True

    def update(self, context, table_info, field_list=None):
        if not field_list:
            field_list = self.__field_list

        query_builder = collections.deque()
        query_builder.append(
            "UPDATE {} SET ".format(self.SYSTEM_TABLE_TABLE_INFO)
        )

        query_builder.append(
            ",".join(
                [
                    '"{}"=\'{}\''.format(field, getattr(table_info, field))
                    for field in field_list
                ]
            )
        )

        query_builder.append(
            " WHERE tenant='{}' AND name='{}' IF exists=1".format(
                context.tenant, table_info.name
            )
        )

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        if not result[0]['[applied]']:
            raise exception.TableNotExistsException(
                "Table {} is not exists".format(table_info.name)
            )
        return True

    def save(self, context, table_info):
        query_builder = collections.deque()
        query_builder.append(
            'INSERT INTO {} '
            '(exists, tenant, name, "schema", status, internal_name)'
            "VALUES(1, '{}', '{}'".format(
                self.SYSTEM_TABLE_TABLE_INFO, context.tenant, table_info.name
            )
        )

        if table_info.schema:
            query_builder.append(",'{}'".format(table_info.schema.to_json()))
        else:
            query_builder.append(",null")

        if table_info.status:
            query_builder.append(",'{}'".format(table_info.status))
        else:
            query_builder.append(",null")

        try:
            internal_name = table_info.internal_name
        except AttributeError:
            internal_name = None

        if internal_name:
            query_builder.append(",'{}'".format(table_info.internal_name))
        else:
            query_builder.append(",null")

        query_builder.append(") IF NOT EXISTS")

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True
        )

        if not result[0]['[applied]']:
            raise exception.TableAlreadyExistsException(
                "Table {} already exists".format(table_info.name)
            )

        self._save_table_info_to_cache(context, table_info)
        return True

    def delete(self, context, table_name):
        query = (
            "DELETE FROM {}"
            " WHERE tenant='{}' AND name='{}'".format(
                self.SYSTEM_TABLE_TABLE_INFO, context.tenant, table_name
            )
        )
        self.__cluster_handler.execute_query(query, consistent=True)

        return True
