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
from decimal import Decimal
import json
import binascii
import time

from cassandra import decoder
from cassandra import AlreadyExists

from magnetodb.common.cassandra import cluster
from magnetodb.common.exception import BackendInteractionException
from magnetodb.common.exception import TableNotExistsException
from magnetodb.common.exception import TableAlreadyExistsException
from magnetodb.openstack.common import importutils
from magnetodb.openstack.common import log as logging
from magnetodb.storage import models

LOG = logging.getLogger(__name__)


class CassandraStorageImpl(object):

    STORAGE_TO_CASSANDRA_TYPES = {
        models.ATTRIBUTE_TYPE_STRING: 'text',
        models.ATTRIBUTE_TYPE_NUMBER: 'decimal',
        models.ATTRIBUTE_TYPE_BLOB: 'blob',
        models.ATTRIBUTE_TYPE_STRING_SET: 'set<text>',
        models.ATTRIBUTE_TYPE_NUMBER_SET: 'set<decimal>',
        models.ATTRIBUTE_TYPE_BLOB_SET: 'set<blob>'
    }

    CASSANDRA_TO_STORAGE_TYPES = {val: key for key, val
                                  in STORAGE_TO_CASSANDRA_TYPES.iteritems()}

    CONDITION_TO_OP = {
        models.Condition.CONDITION_TYPE_EQUAL: '=',
        models.IndexedCondition.CONDITION_TYPE_LESS: '<',
        models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL: '<=',
        models.IndexedCondition.CONDITION_TYPE_GREATER: '>',
        models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL: '>=',
    }

    USER_PREFIX = 'user_'
    SYSTEM_PREFIX = 'system_'

    USER_TABLE_PREFIX = USER_PREFIX
    SYSTEM_TABLE_PREFIX = SYSTEM_PREFIX

    SYSTEM_TABLE_SCHEMA_STATUS = SYSTEM_TABLE_PREFIX + "table_schema_status"

    USER_COLUMN_PREFIX = USER_PREFIX
    SYSTEM_COLUMN_PREFIX = SYSTEM_PREFIX

    SYSTEM_COLUMN_ATTRS = SYSTEM_COLUMN_PREFIX + 'attrs'
    SYSTEM_COLUMN_ATTR_TYPES = SYSTEM_COLUMN_PREFIX + 'attr_types'
    SYSTEM_COLUMN_ATTR_EXIST = SYSTEM_COLUMN_PREFIX + 'attr_exist'
    SYSTEM_COLUMN_HASH = SYSTEM_COLUMN_PREFIX + 'hash'

    SYSTEM_COLUMN_HASH_INDEX_NAME = (
        SYSTEM_COLUMN_HASH + "_internal_index"
    )

    __table_schema_cache = {}

    @classmethod
    def _save_table_schema_to_cache(cls, tenant, table_name, schema):
        tenant_tables_cache = cls.__table_schema_cache.get(tenant)
        if tenant_tables_cache is None:
            tenant_tables_cache = {}
            cls.__table_schema_cache[tenant] = tenant_tables_cache
        tenant_tables_cache[table_name] = schema

    @classmethod
    def _get_table_schema_from_cache(cls, tenant, table_name,):
        tenant_tables_cache = cls.__table_schema_cache.get(tenant)
        if tenant_tables_cache is None:
            return None
        return tenant_tables_cache.get(table_name)

    @classmethod
    def _remove_table_schema_from_cache(cls, tenant, table_name):
        tenant_tables_cache = cls.__table_schema_cache.get(tenant)
        if tenant_tables_cache is None:
            return None

        return tenant_tables_cache.pop(table_name, None)

    def __init__(self, contact_points=("127.0.0.1",),
                 port=9042,
                 compression=True,
                 auth_provider=None,
                 load_balancing_policy=None,
                 reconnection_policy=None,
                 default_retry_policy=None,
                 conviction_policy_factory=None,
                 metrics_enabled=False,
                 connection_class=None,
                 ssl_options=None,
                 sockopts=None,
                 cql_version=None,
                 executor_threads=2,
                 max_schema_agreement_wait=10,
                 control_connection_timeout=2.0,
                 query_timeout=10.0):

        if connection_class:
            connection_class = importutils.import_class(connection_class)

        self.cluster = cluster.Cluster(
            contact_points=contact_points,
            port=port,
            compression=compression,
            auth_provider=auth_provider,
            load_balancing_policy=load_balancing_policy,
            reconnection_policy=reconnection_policy,
            default_retry_policy=default_retry_policy,
            conviction_policy_factory=conviction_policy_factory,
            metrics_enabled=metrics_enabled,
            connection_class=connection_class,
            ssl_options=ssl_options,
            sockopts=sockopts,
            cql_version=cql_version,
            executor_threads=executor_threads,
            max_schema_agreement_wait=max_schema_agreement_wait,
            control_connection_timeout=control_connection_timeout,
            schema_change_listeners=(self.schema_change_listener,)
        )

        self.session = self.cluster.connect()
        self.session.row_factory = decoder.dict_factory
        self.session.default_timeout = query_timeout

    def schema_change_listener(self, event):
        LOG.debug("Schema change event captured: %s" % event)

        tenant = event.get('keyspace')
        table_name = event.get('table')

        if (tenant is None) or (table_name is None):
            return

        if event['change_type'] == "DROPPED":
            self._remove_table_schema_from_cache(tenant, table_name)

    def _execute_query(self, query, consistent=False):
        try:
            if consistent:
                query = cluster.SimpleStatement(
                    query, consistency_level=cluster.ConsistencyLevel.QUORUM
                )

            LOG.debug("Executing query {}".format(query))
            return self.session.execute(query)
        except AlreadyExists as e:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.error(msg)
            raise TableAlreadyExistsException(msg)
        except Exception as e:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.error(msg)
            raise BackendInteractionException(
                msg)

    def _wait_for_table_status(self, keyspace_name, table_name,
                               expected_exists, indexed_column_names=None):
        LOG.debug("Start waiting for table status changing...")

        while True:
            keyspace_meta = self.cluster.metadata.keyspaces.get(keyspace_name)

            if keyspace_meta is None:
                raise BackendInteractionException(
                    "Keyspace '{}' does not exist".format(keyspace_name)
                )

            table_meta = keyspace_meta.tables.get(table_name)
            if expected_exists == (table_meta is not None):
                if table_meta is None or not indexed_column_names:
                    break

                for indexed_column in indexed_column_names:
                    column = table_meta.columns.get(indexed_column)
                    if not column.index:
                        break
                else:
                    break
            LOG.debug("Table status isn't correct"
                      "(expected_exists: %s, table_meta: %s)."
                      " Wait and check again" %
                      (expected_exists, table_meta))
            time.sleep(1)

        LOG.debug("Table status is correct"
                  "(expected_exists: %s, table_meta: %s)" %
                  (expected_exists, table_meta))

    def create_table(self, context, table_schema):
        """
        Creates table

        @param context: current request context
        @param table_schema: TableSchema instance which define table to create

        @raise BackendInteractionException
        """

        cas_table_name = self.USER_TABLE_PREFIX + table_schema.table_name

        user_columns = [
            '"{}{}" {}'.format(
                self.USER_COLUMN_PREFIX, attr_name,
                self.STORAGE_TO_CASSANDRA_TYPES[attr_type]
            )
            for attr_name, attr_type in
            table_schema.attribute_type_map.iteritems()
        ]

        hash_name = table_schema.key_attributes[0]
        hash_type = table_schema.attribute_type_map[hash_name]

        cassandra_hash_type = self.STORAGE_TO_CASSANDRA_TYPES[hash_type]

        key_attrs = [
            "\"{}{}\"".format(self.USER_COLUMN_PREFIX, name)
            for name in table_schema.key_attributes if name
        ]

        key_count = len(key_attrs)

        if key_count < 1 or key_count > 2:
            raise BackendInteractionException(
                "Expected 1 or 2 key attribute(s). Found {}: {}".format(
                    key_count, table_schema.key_attributes))

        query = (
            "CREATE TABLE \"{}\".\"{}\" ("
            " {},"
            " \"{}\" map<text, blob>,"
            " \"{}\" map<text, text>,"
            " \"{}\" set<text>,"
            " {} {},"
            " PRIMARY KEY ({})"
            ")".format(
                context.tenant, cas_table_name,
                ",".join(user_columns),
                self.SYSTEM_COLUMN_ATTRS,
                self.SYSTEM_COLUMN_ATTR_TYPES,
                self.SYSTEM_COLUMN_ATTR_EXIST,
                self.SYSTEM_COLUMN_HASH, cassandra_hash_type,
                ','.join(key_attrs)
            )
        )

        try:
            self._execute_query(query)

            LOG.debug("Create Table CQL request executed. "
                      "Waiting for schema agreement...")

            self._wait_for_table_status(keyspace_name=context.tenant,
                                        table_name=cas_table_name,
                                        expected_exists=True)

            LOG.debug("Waiting for schema agreement... Done")

            for index_name, index_def in (
                    table_schema.index_def_map.iteritems()):
                self._create_index(context.tenant, cas_table_name,
                                   self.USER_COLUMN_PREFIX +
                                   index_def.attribute_to_index,
                                   index_name)

            self._create_index(
                context.tenant, cas_table_name, self.SYSTEM_COLUMN_HASH,
                self.SYSTEM_COLUMN_HASH_INDEX_NAME)

        except Exception as e:
            LOG.error("Table {} creation failed.".format(
                table_schema.table_name))
            LOG.error(e.message)
            # LOG.error("Table {} creation failed. Cleaning up...".format(
            #     table_schema.table_name))
            #
            # try:
            #     self.delete_table(context, table_schema.table_name)
            # except Exception:
            #     LOG.error("Failed table {} was not deleted".format(
            #         table_schema.table_name))

            raise e

    def _create_index(self, keyspace_name, table_name, indexed_attr,
                      index_name=""):
        if index_name:
            index_name = "_".join((table_name, index_name))

        query = "CREATE INDEX {} ON \"{}\".\"{}\" (\"{}\")".format(
            index_name, keyspace_name, table_name, indexed_attr)

        self._execute_query(query)

        self._wait_for_table_status(keyspace_name=keyspace_name,
                                    table_name=table_name,
                                    expected_exists=True,
                                    indexed_column_names=(indexed_attr,))

    def delete_table(self, context, table_name):
        """
        Creates table

        @param context: current request context
        @param table_name: String, name of table to delete

        @raise BackendInteractionException
        """
        cas_table_name = self.USER_TABLE_PREFIX + table_name

        query = "DROP TABLE \"{}\".\"{}\"".format(context.tenant,
                                                  cas_table_name)

        self._execute_query(query)

        LOG.debug("Delete Table CQL request executed. "
                  "Waiting for schema agreement...")

        self._wait_for_table_status(keyspace_name=context.tenant,
                                    table_name=cas_table_name,
                                    expected_exists=False)

        LOG.debug("Waiting for schema agreement... Done")

    def describe_table(self, context, table_name):
        """
        Describes table

        @param context: current request context
        @param table_name: String, name of table to describes

        @return: TableSchema instance

        @raise BackendInteractionException
        """

        table_schema = self._get_table_schema_from_cache(context.tenant,
                                                         table_name)
        if table_schema:
            return table_schema

        keyspace_meta = self.cluster.metadata.keyspaces.get(context.tenant)

        if keyspace_meta is None:
            raise BackendInteractionException(
                "Keyspace '{}' does not exist".format(context.tenant)
            )

        cas_table_name = self.USER_TABLE_PREFIX + table_name
        table_meta = keyspace_meta.tables.get(cas_table_name)
        if table_meta is None:
            raise TableNotExistsException(
                "Table '{}' does not exist".format(cas_table_name)
            )

        prefix_len = len(self.USER_COLUMN_PREFIX)

        user_columns = [val for key, val
                        in table_meta.columns.iteritems()
                        if key.startswith(self.USER_COLUMN_PREFIX)]

        attribute_type_map = {}
        index_def_map = {}

        for column in user_columns:
            name = column.name[prefix_len:]
            storage_type = self.CASSANDRA_TO_STORAGE_TYPES[column.typestring]
            attribute_type_map[name] = storage_type
            if column.index:
                index_def_map[column.index.name[len(table_name) + 1:]] = (
                    models.IndexDefinition(name)
                )

        hash_key_name = table_meta.partition_key[0].name[prefix_len:]

        key_attrs = [hash_key_name]

        if table_meta.clustering_key:
            range_key_name = table_meta.clustering_key[0].name[prefix_len:]
            key_attrs.append(range_key_name)

        table_schema = models.TableSchema(table_name, attribute_type_map,
                                          key_attrs, index_def_map)

        self._save_table_schema_to_cache(context.tenant, table_name,
                                         table_schema)

        return table_schema

    def list_tables(self, context, exclusive_start_table_name=None,
                    limit=None):
        """
        @param context: current request context
        @param exclusive_start_table_name
        @param limit: limit of returned table names
        @return list of table names

        @raise BackendInteractionException
        """

        query_builder = [
            "SELECT columnfamily_name",
            " FROM system.schema_columnfamilies",
            " WHERE keyspace_name='", context.tenant, "'"
        ]

        if exclusive_start_table_name:
            query_builder += (
                " AND columnfamily_name > '",
                exclusive_start_table_name, "'"
            )

        if limit:
            query_builder += (" LIMIT ", str(limit))

        tables = self._execute_query("".join(query_builder), consistent=True)

        return [row['columnfamily_name'][len(self.USER_TABLE_PREFIX):]
                for row in tables]

    def put_item(self, context, put_request, if_not_exist=False,
                 expected_condition_map=None):
        """
        @param context: current request context
        @param put_request: contains PutItemRequest items to perform
                    put item operation
        @param if_not_exist: put item only is row is new record (It is possible
                    to use only one of if_not_exist and expected_condition_map
                    parameter)
        @param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be put or
                    not

        @return: True if operation performed, otherwise False

        @raise BackendInteractionException
        """
        query = self._get_put_item_query(context, put_request, if_not_exist,
                                         expected_condition_map)

        result = self._execute_query(query, consistent=True)

        return (result is None) or result[0]['[applied]']

    def _get_put_item_query(self, context, put_request, if_not_exist=False,
                            expected_condition_map=None):
        """
        @param context: current request context
        @param put_request: contains PutItemRequest items to perform
                    put item operation
        @param if_not_exist: put item only is row is new record (It is possible
                    to use only one of if_not_exist and expected_condition_map
                    parameter)
        @param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be put or
                    not

        @return: CQL query string

        @raise BackendInteractionException
        """

        schema = self.describe_table(context, put_request.table_name)
        key_attr_names = schema.key_attributes
        put_attr_map = put_request.attribute_map

        types = self._put_types(put_attr_map)
        exists = self._put_exists(put_attr_map)

        hash_key_name = key_attr_names[0]
        encoded_hash_key_value = self._encode_predefined_attr_value(
            put_attr_map[hash_key_name]
        )

        not_processed_predefined_attr_names = set(
            schema.attribute_type_map.keys()
        )

        query_builder = None

        if expected_condition_map:
            query_builder = [
                'UPDATE "', context.tenant, '"."', self.USER_TABLE_PREFIX,
                put_request.table_name, '" SET ']

            dynamic_attr_names = []
            dynamic_attr_values = []

            for name, val in put_attr_map.iteritems():
                if name in key_attr_names:
                    not_processed_predefined_attr_names.remove(name)
                elif name in not_processed_predefined_attr_names:
                    query_builder += (
                        '"', self.USER_COLUMN_PREFIX, name, '"=',
                        self._encode_predefined_attr_value(val), ","
                    )
                    not_processed_predefined_attr_names.remove(name)
                else:
                    dynamic_attr_names.append(name)
                    dynamic_attr_values.append(
                        self._encode_dynamic_attr_value(val)
                    )

            query_builder += (self.SYSTEM_COLUMN_ATTRS, "={")
            if dynamic_attr_values:
                dynamic_value_iter = iter(dynamic_attr_values)
                for name in dynamic_attr_names:
                    query_builder += (
                        "'", name, "':", dynamic_value_iter.next(), ","
                    )
                query_builder.pop()
            query_builder.append("},")

            for name in not_processed_predefined_attr_names:
                query_builder += (
                    '"', self.USER_COLUMN_PREFIX, name,
                    '"=null,'
                )

            query_builder += (
                self.SYSTEM_COLUMN_ATTR_TYPES, "={", types, "},",
                self.SYSTEM_COLUMN_ATTR_EXIST, "={", exists, "},",
                self.SYSTEM_COLUMN_HASH, "=", encoded_hash_key_value,
                ' WHERE "', self.USER_COLUMN_PREFIX, hash_key_name, '"=',
                encoded_hash_key_value
            )

            if len(key_attr_names) == 2:
                range_key_name = key_attr_names[1]
                encoded_range_key_value = self._encode_predefined_attr_value(
                    put_attr_map[range_key_name]
                )

                query_builder += (
                    ' AND "', self.USER_COLUMN_PREFIX, range_key_name, '"=',
                    encoded_range_key_value
                )

            if expected_condition_map:
                query_builder.append(" IF ")
                self._append_expected_conditions(
                    expected_condition_map, schema, query_builder
                )
        else:
            query_builder = [
                'INSERT INTO "', context.tenant, '"."',
                self.USER_TABLE_PREFIX, put_request.table_name, '" ('
            ]
            attr_values = []
            dynamic_attr_names = []
            dynamic_attr_values = []
            for name, val in put_attr_map.iteritems():
                if name in not_processed_predefined_attr_names:
                    query_builder += (
                        '"', self.USER_COLUMN_PREFIX, name, '",'
                    )
                    attr_values.append(self._encode_predefined_attr_value(val))
                    not_processed_predefined_attr_names.remove(name)
                else:
                    dynamic_attr_names.append(name)
                    dynamic_attr_values.append(
                        self._encode_dynamic_attr_value(val)
                    )

            query_builder += (
                self.SYSTEM_COLUMN_ATTRS, ",",
                self.SYSTEM_COLUMN_ATTR_TYPES, ",",
                self.SYSTEM_COLUMN_ATTR_EXIST, ",",
                self.SYSTEM_COLUMN_HASH,
                ") VALUES("
            )

            for attr_value in attr_values:
                query_builder += (attr_value, ",")

            query_builder.append("{")

            if dynamic_attr_values:
                dynamic_value_iter = iter(dynamic_attr_values)
                for name in dynamic_attr_names:
                    query_builder += (
                        "'", name, "':" + dynamic_value_iter.next(), ","
                    )
                query_builder.pop()

            query_builder += (
                "},{", types, "},{" + exists + "},", encoded_hash_key_value
            )
            query_builder.append(")")
            if if_not_exist:
                query_builder.append(" IF NOT EXISTS")

        return "".join(query_builder)

    def _put_types(self, attribute_map):
        return ','.join((
            "'{}':'{}'".format(attr, self.STORAGE_TO_CASSANDRA_TYPES[val.type])
            for attr, val
            in attribute_map.iteritems()))

    def _put_exists(self, attribute_map):
        return ','.join((
            "'{}'".format(attr)
            for attr, _
            in attribute_map.iteritems()))

    def delete_item(self, context, delete_request,
                    expected_condition_map=None):
        """
        @param context: current request context
        @param delete_request: contains DeleteItemRequest items to perform
                    delete item operation
        @param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be deleted
                    or not

        @return: True if operation performed, otherwise False (if operation was
                    skipped by out of date timestamp, it is considered as
                    successfully performed)

        @raise BackendInteractionException
        """
        query = self._get_delete_item_query(context, delete_request,
                                            expected_condition_map)

        result = self._execute_query(query, consistent=True)

        return (result is None) or result[0]['[applied]']

    def _get_delete_item_query(self, context, delete_request,
                               expected_condition_map=None):
        """
        @param context: current request context
        @param delete_request: contains DeleteItemRequest items to perform
                    delete item operation
        @param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be deleted
                    or not

        @return: CQL query string

        @raise BackendInteractionException
        """
        query_builder = [
            'DELETE FROM "', context.tenant, '"."', self.USER_TABLE_PREFIX,
            delete_request.table_name, '" WHERE '
        ]

        query_builder.append(
            self._primary_key_as_string(delete_request.key_attribute_map)
        )

        if expected_condition_map:
            schema = self.describe_table(context, delete_request.table_name)
            query_builder.append(" IF ")
            self._append_expected_conditions(
                expected_condition_map, schema, query_builder
            )

        return "".join(query_builder)

    def _compact_indexed_condition(self, cond_list):
        left_condition = None
        right_condition = None
        exact_condition = None

        assert cond_list

        for condition in cond_list:
            if condition.type == models.IndexedCondition.CONDITION_TYPE_EQUAL:
                if (exact_condition is not None and
                        condition.arg.value != exact_condition.arg.value):
                    return None
                exact_condition = condition
            elif condition.is_left_border():
                if left_condition is None:
                    left_condition = condition
                elif condition.is_strict_border():
                    if condition.arg.value >= left_condition.arg.value:
                        left_condition = condition
                else:
                    if condition.arg.value > left_condition.arg.value:
                        left_condition = condition
            elif condition.is_right_border():
                if right_condition is None:
                    right_condition = condition
                elif condition.is_strict():
                    if condition.arg.value <= right_condition.arg.value:
                        right_condition = condition
                else:
                    if condition.arg.value < right_condition.arg.value:
                        right_condition = condition

        if exact_condition is not None:
            if left_condition is not None:
                if left_condition.is_strict():
                    if left_condition.arg.value >= exact_condition.arg.value:
                        return None
                else:
                    if left_condition.arg.value > exact_condition.arg.value:
                        return None
            if right_condition is not None:
                if right_condition.is_strict():
                    if right_condition.arg.value <= exact_condition.arg.value:
                        return None
                else:
                    if right_condition.arg.value < exact_condition.arg.value:
                        return None
            return [exact_condition]
        elif left_condition is not None:
            if right_condition is not None:
                if (left_condition.is_strict_border() or
                        right_condition.is_strict_border()):
                    if left_condition.arg.value >= right_condition.arg.value:
                        return None
                else:
                    if left_condition.arg.value > right_condition.arg.value:
                        return None
                return [left_condition, right_condition]
            else:
                return [left_condition]

        assert right_condition is not None

        return [right_condition]

    def _append_indexed_condition(self, attr_name, condition, query_builder,
                                  column_prefix=USER_COLUMN_PREFIX):
        op = self.CONDITION_TO_OP[condition.type]
        query_builder += (
            '"', column_prefix, attr_name, '"', op,
            self._encode_predefined_attr_value(condition.arg)
        )

    def _append_hash_key_indexed_condition(
            self, attr_name, condition, query_builder,
            column_prefix=USER_COLUMN_PREFIX):
        if condition.type == models.IndexedCondition.CONDITION_TYPE_EQUAL:
            self._append_indexed_condition(
                attr_name, condition, query_builder, column_prefix
            )
        else:
            op = self.CONDITION_TO_OP[condition.type]
            query_builder += (
                'token("', column_prefix, attr_name, '")', op, "token(",
                self._encode_predefined_attr_value(condition.arg), ")"
            )

    def _append_expected_conditions(self, expected_condition_map, schema,
                                    query_builder):
        init_length = len(query_builder)

        for attr_name, cond_list in expected_condition_map.iteritems():
            for condition in cond_list:
                self._append_expected_condition(
                    attr_name, condition, query_builder,
                    attr_name in schema.attribute_type_map
                )
                query_builder.append(" AND ")

        if len(query_builder) > init_length:
            query_builder.pop()

    def _append_expected_condition(self, attr, condition, query_builder,
                                   is_predefined):
        if condition.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            if condition.arg:
                query_builder += (
                    self.SYSTEM_COLUMN_ATTR_EXIST, "={'", attr, "'}"
                )
            else:
                if is_predefined:
                    query_builder += (
                        '"', self.USER_COLUMN_PREFIX, attr, '"=null'
                    )
                else:
                    query_builder += (
                        self.SYSTEM_COLUMN_ATTRS, "['", attr, "']=null"
                    )
        elif condition.type == models.ExpectedCondition.CONDITION_TYPE_EQUAL:
            if is_predefined:
                query_builder += (
                    '"', self.USER_COLUMN_PREFIX, attr, '"=',
                    self._encode_predefined_attr_value(condition.arg)
                )
            else:
                query_builder += (
                    self.SYSTEM_COLUMN_ATTRS, "['", attr, "']=",
                    self._encode_dynamic_attr_value(condition.arg)
                )
        else:
            assert False

    def _primary_key_as_string(self, key_map):
        return " AND ".join((
            "\"{}\"={}".format(self.USER_COLUMN_PREFIX + attr_name,
                               self._encode_predefined_attr_value(attr_value))
            for attr_name, attr_value in key_map.iteritems()))

    def _get_batch_begin_query(self, durable=True):
        if durable:
            return 'BEGIN BATCH'
        return 'BEGIN UNLOGGED BATCH'

    def _get_batch_apply_query(self):
        return 'APPLY BATCH;'

    def execute_write_batch(self, context, write_request_list, durable=True):
        """
        @param context: current request context
        @param write_request_list: contains WriteItemBatchableRequest items to
                    perform batch
        @param durable: if True, batch will be fully performed or fully
                    skipped. Partial batch execution isn't allowed

        @return: True if operation performed, otherwise False

        @raise BackendInteractionException
        """
        if not write_request_list:
            # TODO(achudnovets): or raise BackendInteractionException?
            return False

        query_builder = collections.deque()
        query_builder.append(self._get_batch_begin_query(durable))

        for req in write_request_list:
            if isinstance(req, models.PutItemRequest):
                query_builder.append(self._get_put_item_query(context, req))
            elif isinstance(req, models.DeleteItemRequest):
                query_builder.append(
                    self._get_delete_item_query(context, req))
            else:
                raise BackendInteractionException('Wrong WriteItemRequest')

        query_builder.append(self._get_batch_apply_query())

        result = self._execute_query('\n'.join(query_builder),
                                     consistent=True)
        return (result is None) or result[0]['[applied]']

    def update_item(self, context, table_name, key_attribute_map,
                    attribute_action_map, expected_condition_map=None):
        """
        @param context: current request context
        @param table_name: String, name of table to delete item from
        @param key_attribute_map: key attribute name to
                    AttributeValue mapping. It defines row it to update item
        @param attribute_action_map: attribute name to UpdateItemAction
                    instance mapping. It defines actions to perform for each
                    given attribute
        @param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be updated
                    or not
        @return: True if operation performed, otherwise False

        @raise BackendInteractionException
        """
        attribute_action_map = attribute_action_map or {}

        schema = self.describe_table(context, table_name)
        set_clause = self._updates_as_string(
            schema, key_attribute_map, attribute_action_map)

        where = self._primary_key_as_string(key_attribute_map)

        query_builder = [
            'UPDATE "', context.tenant, '"."', self.USER_TABLE_PREFIX,
            table_name, '" SET ', set_clause, " WHERE ", where
        ]

        if expected_condition_map:
            query_builder.append(" IF ")
            self._append_expected_conditions(
                expected_condition_map, schema, query_builder
            )

        result = self._execute_query("".join(query_builder), consistent=True)

        return (result is None) or result[0]['[applied]']

    def _updates_as_string(self, schema, key_attribute_map, update_map):
        set_clause = ", ".join({
            self._update_as_string(attr, update,
                                   attr in schema.attribute_type_map)
            for attr, update in update_map.iteritems()})

        #update system_hash
        hash_name = schema.key_attributes[0]
        hash_value = self._encode_predefined_attr_value(
            key_attribute_map[hash_name]
        )

        set_clause += ",\"{}\"={}".format(self.SYSTEM_COLUMN_HASH, hash_value)

        return set_clause

    def _update_as_string(self, attr, update, is_predefined):
        if is_predefined:
            name = "\"{}\"".format(self.USER_COLUMN_PREFIX + attr)
        else:
            name = "\"{}\"['{}']".format(self.SYSTEM_COLUMN_ATTRS, attr)

        # delete value
        if (update.action == models.UpdateItemAction.UPDATE_ACTION_DELETE
            or (update.action == models.UpdateItemAction.UPDATE_ACTION_PUT
                and (not update.value or not update.value.value))):
            value = 'null'

            type_update = "\"{}\"['{}'] = null".format(
                self.SYSTEM_COLUMN_ATTR_TYPES, attr)

            exists = "\"{}\" = \"{}\" - {{'{}'}}".format(
                self.SYSTEM_COLUMN_ATTR_EXIST,
                self.SYSTEM_COLUMN_ATTR_EXIST, attr)
        # put or add
        else:
            type_update = "\"{}\"['{}'] = '{}'".format(
                self.SYSTEM_COLUMN_ATTR_TYPES, attr,
                self.STORAGE_TO_CASSANDRA_TYPES[update.value.type])

            exists = "\"{}\" = \"{}\" + {{'{}'}}".format(
                self.SYSTEM_COLUMN_ATTR_EXIST,
                self.SYSTEM_COLUMN_ATTR_EXIST, attr)

            value = (
                self._encode_predefined_attr_value(update.value)
                if is_predefined else
                self._encode_dynamic_attr_value(update.value)
            )

        op = '='
        value_update = "{} {} {}".format(name, op, value)

        return ", ".join((value_update, type_update, exists))

    def _encode_predefined_attr_value(self, attr_value):
        if attr_value is None:
            return 'null'
        if attr_value.type.collection_type:
            values = ','.join(map(
                lambda el: self._encode_single_value_as_predefined_attr(
                    el, attr_value.type.element_type),
                attr_value.value
            ))
            return '{{{}}}'.format(values)
        else:
            return self._encode_single_value_as_predefined_attr(
                attr_value.value, attr_value.type.element_type
            )

    @staticmethod
    def _encode_single_value_as_predefined_attr(value, element_type):
        if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
            return "'{}'".format(value)
        elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            return str(value)
        elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            return "0x{}".format(binascii.hexlify(value))
        else:
            assert False, "Value wasn't formatted for cql query"

    def _encode_dynamic_attr_value(self, attr_value):
        if attr_value is None:
            return 'null'

        val = attr_value.value
        if attr_value.type.collection_type:
            val = map(
                lambda el: self._encode_single_value_as_dynamic_attr(
                    el, attr_value.type.element_type
                ),
                val
            )
            val.sort()
        else:
            val = self._encode_single_value_as_dynamic_attr(
                val, attr_value.type.element_type)
        return "0x{}".format(binascii.hexlify(json.dumps(val)))

    @staticmethod
    def _encode_single_value_as_dynamic_attr(value, element_type):
        if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
            return value
        elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            return str(value)
        elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            return value
        else:
            assert False, "Value wasn't formatted for cql query"

    @staticmethod
    def _decode_value(value, storage_type, is_predefined):
        if not is_predefined:
            value = json.loads(value)

        return models.AttributeValue(storage_type, value)

    @staticmethod
    def _decode_single_value(value, element_type):
        if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
            return value
        elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            return Decimal(value)
        elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            return value
        else:
            assert False, "Value wasn't formatted for cql query"

    def select_item(self, context, table_name, indexed_condition_map=None,
                    select_type=None, index_name=None, limit=None,
                    exclusive_start_key=None, consistent=True,
                    order_type=None):
        """
        @param context: current request context
        @param table_name: String, name of table to get item from
        @param indexed_condition_map: indexed attribute name to
                    IndexedCondition instance mapping. It defines rows
                    set to be selected
        @param select_type: SelectType instance. It defines with attributes
                    will be returned. If not specified, default will be used:
                        SelectType.all() for query on table and
                        SelectType.all_projected() for query on index
        @param index_name: String, name of index to search with
        @param limit: maximum count of returned values
        @param exclusive_start_key: key attribute names to AttributeValue
                    instance
        @param consistent: define is operation consistent or not (by default it
                    is not consistent)
        @param order_type: defines order of returned rows, if 'None' - default
                    order will be used

        @return SelectResult instance

        @raise BackendInteractionException
        """

        schema = self.describe_table(context, table_name)
        hash_name = schema.key_attributes[0]

        try:
            range_name = schema.key_attributes[1]
        except IndexError:
            range_name = None

        select_type = select_type or models.SelectType.all()

        query_builder = [
            "SELECT ", 'COUNT(*)' if select_type.is_count else '*', ' FROM "',
            context.tenant, '"."', self.USER_TABLE_PREFIX, table_name, '"'
        ]

        if exclusive_start_key:
            indexed_condition_map = indexed_condition_map or {}

            exclusive_hash_key_value = exclusive_start_key[hash_name]
            exclusive_range_key_value = exclusive_start_key.get(range_name,
                                                                None)
            if exclusive_range_key_value:
                range_key_cond_list = indexed_condition_map.get(
                    range_name, None
                )
                if range_key_cond_list is None:
                    range_key_cond_list = []
                    indexed_condition_map[range_name] = range_key_cond_list

                range_key_cond_list.append(
                    models.IndexedCondition.lt(exclusive_range_key_value)
                    if order_type == models.ORDER_TYPE_DESC else
                    models.IndexedCondition.gt(exclusive_range_key_value)
                )

                hash_key_cond_list = indexed_condition_map.get(
                    hash_name, None
                )
                if hash_key_cond_list is None:
                    hash_key_cond_list = []
                    indexed_condition_map[hash_name] = hash_key_cond_list

                hash_key_cond_list.append(
                    models.IndexedCondition.eq(exclusive_hash_key_value)
                )
            else:
                hash_key_cond_list = indexed_condition_map.get(
                    hash_name, None
                )
                if hash_key_cond_list is None:
                    hash_key_cond_list = []
                    indexed_condition_map[hash_name] = hash_key_cond_list

                hash_key_cond_list.append(
                    models.IndexedCondition.lt(exclusive_hash_key_value)
                    if order_type == models.ORDER_TYPE_DESC else
                    models.IndexedCondition.gt(exclusive_hash_key_value)
                )

        pre_condition_str = " WHERE "

        if indexed_condition_map:
            hash_cond_list = None
            for attr, cond_list in indexed_condition_map.iteritems():
                active_cond_list = self._compact_indexed_condition(cond_list)
                if active_cond_list is None:
                    return models.SelectResult(count=0)

                if attr == hash_name:
                    hash_cond_list = active_cond_list
                    for active_cond in active_cond_list:
                        query_builder.append(pre_condition_str)
                        pre_condition_str = " AND "
                        self._append_hash_key_indexed_condition(
                            attr, active_cond, query_builder
                        )
                else:
                    for active_cond in active_cond_list:
                        query_builder.append(pre_condition_str)
                        pre_condition_str = " AND "
                        self._append_indexed_condition(
                            attr, active_cond, query_builder
                        )

            if (hash_cond_list is not None and
                    len(hash_cond_list) == 1 and
                    hash_cond_list[0].type ==
                    models.IndexedCondition.CONDITION_TYPE_EQUAL):
                query_builder.append(pre_condition_str)
                self._append_indexed_condition(
                    self.SYSTEM_COLUMN_HASH, hash_cond_list[0],
                    query_builder, column_prefix="")

        #add limit
        if limit:
            query_builder += (" LIMIT ", str(limit))

        #add ordering
        if order_type and range_name:
            query_builder += (
                ' ORDER BY "', self.USER_COLUMN_PREFIX, range_name, '" ',
                order_type
            )

        #add allow filtering
        query_builder.append(" ALLOW FILTERING")

        rows = self._execute_query("".join(query_builder), consistent)

        if select_type.is_count:
            return models.SelectResult(count=rows[0]['count'])

        # process results

        prefix_len = len(self.USER_COLUMN_PREFIX)
        result = []

        # TODO ikhudoshyn: if select_type.is_all_projected,
        # get list of projected attrs by index_name from metainfo

        attributes_to_get = select_type.attributes

        for row in rows:
            record = {}

            #add predefined attributes
            for key, val in row.iteritems():
                if key.startswith(self.USER_COLUMN_PREFIX) and val:
                    name = key[prefix_len:]
                    if not attributes_to_get or name in attributes_to_get:
                        storage_type = schema.attribute_type_map[name]
                        record[name] = self._decode_value(
                            val, storage_type, True)

            #add dynamic attributes (from SYSTEM_COLUMN_ATTRS dict)
            types = row[self.SYSTEM_COLUMN_ATTR_TYPES]
            attrs = row[self.SYSTEM_COLUMN_ATTRS] or {}
            for name, val in attrs.iteritems():
                if not attributes_to_get or name in attributes_to_get:
                    typ = types[name]
                    storage_type = self.CASSANDRA_TO_STORAGE_TYPES[typ]
                    record[name] = self._decode_value(
                        val, storage_type, False)

            result.append(record)

        count = len(result)
        if limit and count == limit:
            last_evaluated_key = {hash_name: result[-1][hash_name]}

            if range_name:
                last_evaluated_key[range_name] = result[-1][range_name]
        else:
            last_evaluated_key = None

        return models.SelectResult(items=result,
                                   last_evaluated_key=last_evaluated_key,
                                   count=count)

    def scan(self, context, table_name, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None, consistent=False):
        """
        @param context: current request context
        @param table_name: String, name of table to get item from
        @param condition_map: indexed attribute name to
                    IndexedCondition instance mapping. It defines rows
                    set to be selected
        @param limit: maximum count of returned values
        @param exclusive_start_key: key attribute names to AttributeValue
                    instance
        @param consistent: define is operation consistent or not (by default it
                    is not consistent)

        @return list of attribute name to AttributeValue mappings

        @raise BackendInteractionException
        """

        condition_map = condition_map or {}

        key_conditions = {}

        schema = self.describe_table(context, table_name)
        hash_name = schema.key_attributes[0]

        try:
            range_name = schema.key_attributes[1]
        except IndexError:
            range_name = None

        if (hash_name in condition_map
            and condition_map[hash_name].type ==
                models.Condition.CONDITION_TYPE_EQUAL):

            key_conditions[hash_name] = condition_map[hash_name]

            if (range_name and range_name in condition_map
                and condition_map[range_name].type in
                    models.IndexedCondition._allowed_types):

                key_conditions[range_name] = condition_map[range_name]

        selected = self.select_item(context, table_name, key_conditions,
                                    models.SelectType.all(), limit=limit,
                                    consistent=consistent,
                                    exclusive_start_key=exclusive_start_key)

        if (range_name and exclusive_start_key
                and range_name in exclusive_start_key
                and (not limit or limit > selected.count)):

            del exclusive_start_key[range_name]

            limit2 = limit - selected.count if limit else None

            selected2 = self.select_item(
                context, table_name, key_conditions,
                models.SelectType.all(), limit=limit2,
                consistent=consistent,
                exclusive_start_key=exclusive_start_key)

            selected = models.SelectResult(
                items=selected.items + selected2.items,
                last_evaluated_key=selected2.last_evaluated_key,
                count=selected.count + selected2.count
            )

        scanned_count = selected.count

        if selected.items:
            filtered_items = filter(
                lambda item: self._conditions_satisfied(
                    item, condition_map),
                selected.items)
            count = len(filtered_items)
        else:
            filtered_items = []
            count = selected.count

        if attributes_to_get and filtered_items:
            for item in filtered_items:
                for attr in item.keys():
                    if not attr in attributes_to_get:
                        del item[attr]

        filtered = models.ScanResult(
            items=filtered_items,
            last_evaluated_key=selected.last_evaluated_key,
            count=count, scanned_count=scanned_count)

        return filtered

    def _conditions_satisfied(self, row, cond_map=None):
        if not cond_map:
            return True

        for attr_name, cond_list in cond_map.iteritems():
            for cond in cond_list:
                if not self._condition_satisfied(
                        row.get(attr_name, None), cond):
                    return False
        return True

    @staticmethod
    def _condition_satisfied(attr_val, cond):

        if cond.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            return cond.arg == bool(attr_val)

        if not attr_val:
            return False

        if cond.type == models.Condition.CONDITION_TYPE_EQUAL:
            return (attr_val.type == cond.arg.type and
                    attr_val.value == cond.arg.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_LESS:
            return (attr_val.type == cond.arg.type and
                    attr_val.value < cond.arg.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL:
            return (attr_val.type == cond.arg.type and
                    attr_val.value <= cond.arg.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_GREATER:
            return (attr_val.type == cond.arg.type and
                    attr_val.value > cond.arg.value)

        if (cond.type ==
                models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL):
            return (attr_val.type == cond.arg.type and
                    attr_val.value >= cond.arg.value)

        if cond.type == models.ScanCondition.CONDITION_TYPE_NOT_EQUAL:
            return (attr_val.type != cond.arg.type or
                    attr_val.value != cond.arg.value)

        if cond.type == models.ScanCondition.CONDITION_TYPE_CONTAINS:
            assert not cond.arg.type.collection_type
            if attr_val.type.element_type != cond.arg.type.element_type:
                return False

            return cond.arg.value in attr_val.value

        if cond.type == models.ScanCondition.CONDITION_TYPE_NOT_CONTAINS:
            assert not cond.arg.type.collection_type
            if attr_val.type.element_type != cond.arg.type.element_type:
                return False

            return cond.arg.value not in attr_val.value

        if cond.type == models.ScanCondition.CONDITION_TYPE_IN:
            cond_arg = cond.arg or []

            return attr_val in cond_arg

        return False
