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


class CassandraStorageImpl():

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

    USER_COLUMN_PREFIX = 'user_'
    SYSTEM_COLUMN_PREFIX = 'system_'
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

        self._query_timeout = query_timeout

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
            return self.session.execute(query, timeout=self._query_timeout)
        except AlreadyExists as e:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.error(msg)
            raise TableAlreadyExistsException(msg)
        except Exception as e:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.error(msg)
            raise BackendInteractionException(
                msg)

    @staticmethod
    def _quote_strings(strings):
        return map(lambda attr: "\"{}\"".format(attr), strings)

    def _wait_for_table_status(self, tenant, table_name, expected_exists,
                               indexed_column_names=None):
        LOG.debug("Start waiting for table status changing...")

        while True:
            keyspace_meta = self.cluster.metadata.keyspaces.get(tenant)

            if keyspace_meta is None:
                raise BackendInteractionException(
                    "Tenant '{}' does not exist".format(tenant)
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

        query = "CREATE TABLE \"{}\".\"{}\" (".format(context.tenant,
                                                      table_schema.table_name)

        for attr_def in table_schema.attribute_defs:
            query += "\"{}\" {},".format(
                self.USER_COLUMN_PREFIX + attr_def.name,
                self.STORAGE_TO_CASSANDRA_TYPES[attr_def.type])

        query += "\"{}\" map<text, blob>,".format(self.SYSTEM_COLUMN_ATTRS)
        query += "\"{}\" map<text, text>,".format(
            self.SYSTEM_COLUMN_ATTR_TYPES)
        query += "\"{}\" set<text>,".format(self.SYSTEM_COLUMN_ATTR_EXIST)

        prefixed_attrs = [self.USER_COLUMN_PREFIX + name
                          for name in table_schema.key_attributes]

        hash_name = table_schema.key_attributes[0]
        hash_type = [attr.type
                     for attr in table_schema.attribute_defs
                     if attr.name == hash_name][0]

        cassandra_hash_type = self.STORAGE_TO_CASSANDRA_TYPES[hash_type]

        query += "{} {},".format(self.SYSTEM_COLUMN_HASH, cassandra_hash_type)

        key_count = len(prefixed_attrs)

        if key_count < 1 or key_count > 2:
            raise BackendInteractionException(
                "Expected 1 or 2 key attribute(s). Found {}: {}".format(
                    key_count, table_schema.key_attributes))

        primary_key = ','.join(self._quote_strings(prefixed_attrs))
        query += "PRIMARY KEY ({})".format(primary_key)

        query += ")"

        try:
            self._execute_query(query)

            LOG.debug("Create Table CQL request executed. "
                      "Waiting for schema agreement...")

            self._wait_for_table_status(tenant=context.tenant,
                                        table_name=table_schema.table_name,
                                        expected_exists=True)

            LOG.debug("Waiting for schema agreement... Done")

            for index_def in table_schema.index_defs:
                self._create_index(context, table_schema.table_name,
                                   self.USER_COLUMN_PREFIX +
                                   index_def.attribute_to_index,
                                   index_def.index_name)

            self._create_index(
                context, table_schema.table_name, self.SYSTEM_COLUMN_HASH,
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

    def _create_index(self, context, table_name, indexed_attr, index_name=""):
        if index_name:
            index_name = "_".join((table_name, index_name))

        query = "CREATE INDEX {} ON \"{}\".\"{}\" (\"{}\")".format(
            index_name, context.tenant, table_name, indexed_attr)

        self._execute_query(query)

        self._wait_for_table_status(tenant=context.tenant,
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
        query = "DROP TABLE \"{}\".\"{}\"".format(context.tenant, table_name)

        self._execute_query(query)

        LOG.debug("Delete Table CQL request executed. "
                  "Waiting for schema agreement...")

        self._wait_for_table_status(tenant=context.tenant,
                                    table_name=table_name,
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
                "Tenant '{}' does not exist".format(context.tenant)
            )

        table_meta = keyspace_meta.tables.get(table_name)
        if table_meta is None:
            raise TableNotExistsException(
                "Table '{}' does not exist".format(table_name)
            )

        prefix_len = len(self.USER_COLUMN_PREFIX)

        user_columns = [val for key, val
                        in table_meta.columns.iteritems()
                        if key.startswith(self.USER_COLUMN_PREFIX)]

        attr_defs = set()
        index_defs = set()

        for column in user_columns:
            name = column.name[prefix_len:]
            storage_type = self.CASSANDRA_TO_STORAGE_TYPES[column.typestring]
            attr_defs.add(models.AttributeDefinition(name, storage_type))
            if column.index:
                index_defs.add(models.IndexDefinition(
                    column.index.name[len(table_name) + 1:], name)
                )

        hash_key_name = table_meta.partition_key[0].name[prefix_len:]

        key_attrs = [hash_key_name]

        if table_meta.clustering_key:
            range_key_name = table_meta.clustering_key[0].name[prefix_len:]
            key_attrs.append(range_key_name)

        table_schema = models.TableSchema(table_meta.name, attr_defs,
                                          key_attrs, index_defs)

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

        query = "SELECT \"columnfamily_name\""
        query += " FROM \"system\".\"schema_columnfamilies\""

        query += " WHERE \"keyspace_name\" = '{}'".format(context.tenant)

        if exclusive_start_table_name:
            query += " AND \"columnfamily_name\" > '{}'".format(
                exclusive_start_table_name)

        if limit:
            query += " LIMIT {}".format(limit)

        tables = self._execute_query(query, consistent=True)

        return [row['columnfamily_name'] for row in tables]

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

        schema = self.describe_table(context, put_request.table_name)
        predefined_attrs = [attr.name for attr in schema.attribute_defs]
        key_attrs = schema.key_attributes
        attr_map = put_request.attribute_map

        dynamic_values = self._put_dynamic_values(attr_map, predefined_attrs)
        types = self._put_types(attr_map)
        exists = self._put_exists(attr_map)

        hash_name = schema.key_attributes[0]
        hash_value = self._encode_predefined_attr_value(attr_map[hash_name])

        if expected_condition_map:
            attrs = attr_map.keys()
            non_key_attrs = [
                attr for attr in predefined_attrs if attr not in key_attrs]
            unset_attrs = [
                attr for attr in predefined_attrs if attr not in attrs]
            set_clause = ''

            for attr, val in attr_map.iteritems():
                if attr in non_key_attrs:
                    set_clause += '\"{}\" = {},'.format(
                        self.USER_COLUMN_PREFIX + attr,
                        self._encode_value(val, True))
                elif attr in unset_attrs:
                    set_clause += '\"{}\" = null,'.format(
                        self.USER_COLUMN_PREFIX + attr)

            set_clause += '\"{}\" = {{{}}},'.format(
                self.SYSTEM_COLUMN_ATTRS, dynamic_values
            )

            set_clause += '\"{}\" = {{{}}},'.format(
                self.SYSTEM_COLUMN_ATTR_TYPES, types
            )

            set_clause += '\"{}\" = {{{}}},'.format(
                self.SYSTEM_COLUMN_ATTR_EXIST, exists
            )

            set_clause += '\"{}\" = {}'.format(
                self.SYSTEM_COLUMN_HASH, hash_value
            )

            where = ' AND '.join((
                '\"{}\" = {}'.format(
                    self.USER_COLUMN_PREFIX + attr,
                    self._encode_value(val, True))
                for attr, val in attr_map.iteritems()
                if attr in key_attrs
            ))

            query = 'UPDATE \"{}\".\"{}\" SET {} WHERE {}'.format(
                context.tenant, put_request.table_name, set_clause, where
            )

            if_clause = self._conditions_as_string(expected_condition_map)
            query += " IF {}".format(if_clause)

            self.session.execute(query)
        else:
            attrs = ''
            values = ''

            for attr, val in attr_map.iteritems():
                if attr in predefined_attrs:
                    attrs += '\"{}\",'.format(self.USER_COLUMN_PREFIX + attr)
                    values += self._encode_value(val, True) + ','

            attrs += ','.join((
                self.SYSTEM_COLUMN_ATTRS,
                self.SYSTEM_COLUMN_ATTR_TYPES,
                self.SYSTEM_COLUMN_ATTR_EXIST,
                self.SYSTEM_COLUMN_HASH
            ))

            values += ','.join((
                '{{{}}}'.format(dynamic_values),
                '{{{}}}'.format(types),
                '{{{}}}'.format(exists),
                hash_value
            ))

            query = 'INSERT INTO \"{}\".\"{}\" ({}) VALUES ({})'.format(
                context.tenant, put_request.table_name, attrs, values)

            if if_not_exist:
                query += ' IF NOT EXISTS'

            self.session.execute(query)

        return True

    def _put_dynamic_values(self, attribute_map, predefined_attrs):
        return ','.join((
            "'{}':{}".format(attr, self._encode_value(val, False))
            for attr, val
            in attribute_map.iteritems()
            if not attr in predefined_attrs
        ))

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
        query = "DELETE FROM \"{}\".\"{}\" WHERE ".format(
            context.tenant, delete_request.table_name)

        where = self._primary_key_as_string(delete_request.key_attribute_map)

        query += where

        if expected_condition_map:
            if_clause = self._conditions_as_string(expected_condition_map)

            query += " IF " + if_clause

        self._execute_query(query, consistent=True)

        return True

    def _condition_as_string(self, attr, condition):
        name = self.USER_COLUMN_PREFIX + attr

        if condition.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            if condition.arg:
                return "\"{}\"={{\"{}\"}}".format(
                    self.SYSTEM_COLUMN_ATTR_EXIST, attr)
            else:
                return "\"{}\"=null".format(name)
        elif condition.type == models.IndexedCondition.CONDITION_TYPE_BETWEEN:
            first, second = condition.arg
            val1 = self._encode_predefined_attr_value(first)
            val2 = self._encode_predefined_attr_value(second)
            return " \"{}\" >= {} AND \"{}\" <= {}".format(
                name, val1, name, val2)
        elif (condition.type ==
              models.IndexedCondition.CONDITION_TYPE_BEGINS_WITH):
            first = condition.arg
            second = first.value[:-1] + chr(ord(first.value[-1]) + 1)
            second = models.AttributeValue(condition.arg.type, second)
            val1 = self._encode_predefined_attr_value(first)
            val2 = self._encode_predefined_attr_value(second)
            return " \"{}\" >= {} AND \"{}\" < {}".format(
                name, val1, name, val2)
        else:
            op = self.CONDITION_TO_OP[condition.type]
            return '"' + name + '"' + op + self._encode_predefined_attr_value(
                condition.arg
            )

    def _conditions_as_string(self, condition_map):
        return " AND ".join((self._condition_as_string(attr, cond)
                             for attr, cond
                             in condition_map.iteritems()))

    def _primary_key_as_string(self, key_map):
        return " AND ".join((
            "\"{}\"={}".format(self.USER_COLUMN_PREFIX + attr_name,
                               self._encode_predefined_attr_value(attr_value))
            for attr_name, attr_value in key_map.iteritems()))

    def execute_write_batch(self, context, write_request_list, durable=True):
        """
        @param context: current request context
        @param write_request_list: contains WriteItemBatchableRequest items to
                    perform batch
        @param durable: if True, batch will be fully performed or fully
                    skipped. Partial batch execution isn't allowed

        @raise BackendInteractionException
        """
        raise NotImplementedError

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

        query = "UPDATE \"{}\".\"{}\" SET {} WHERE {}".format(
            context.tenant, table_name, set_clause, where
        )

        if expected_condition_map:
            if_clause = self._conditions_as_string(expected_condition_map)
            query += " IF {}".format(if_clause)

        self._execute_query(query, consistent=True)

        return True

    def _updates_as_string(self, schema, key_attribute_map, update_map):
        predefined_attrs = [attr.name for attr in schema.attribute_defs]

        set_clause = ", ".join({
            self._update_as_string(attr, update, attr in predefined_attrs)
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

            value = self._encode_value(update.value, is_predefined)

        op = '='
        value_update = "{} {} {}".format(name, op, value)

        return ", ".join((value_update, type_update, exists))

    def _encode_value(self, attr_value, is_predefined):
        if attr_value is None:
            return 'null'
        elif is_predefined:
            return self._encode_predefined_attr_value(attr_value)
        else:
            return self._encode_dynamic_attr_value(attr_value)

    def _encode_predefined_attr_value(self, attr_value):
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
        val = attr_value.value
        if attr_value.type.collection_type:
            val = map(
                lambda el: self._encode_single_value_as_dynamic_attr(
                    el, attr_value.type.element_type
                ),
                val)
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

        select = 'COUNT(*)' if select_type.is_count else '*'

        query = "SELECT {} FROM \"{}\".\"{}\" ".format(
            select, context.tenant, table_name)

        indexed_condition_map = indexed_condition_map or {}

        token_cond = None
        fixed_range_cond = None

        if exclusive_start_key:
            if range_name in exclusive_start_key:

                fixed_range_cond = self._fixed_range_condition(
                    range_name, indexed_condition_map,
                    exclusive_start_key)

                indexed_condition_map[hash_name] = models.Condition.eq(
                    exclusive_start_key[hash_name])

            else:
                token_cond = 'token(\"{}\")>token({})'.format(
                    self.USER_COLUMN_PREFIX + hash_name,
                    self._encode_predefined_attr_value(
                        exclusive_start_key[hash_name]))

                if hash_name in indexed_condition_map:
                    del indexed_condition_map[hash_name]

        where = self._conditions_as_string(indexed_condition_map)

        if token_cond:
            if where:
                where += ' AND ' + token_cond
            else:
                where = token_cond

        if fixed_range_cond:
            if where:
                where += ' AND ' + fixed_range_cond
            else:
                where = fixed_range_cond

        if where:
            query += " WHERE " + where

        add_filtering, add_system_hash = self._add_filtering_and_sys_hash(
            schema, indexed_condition_map)

        if add_system_hash:
            hash_value = self._encode_predefined_attr_value(
                indexed_condition_map[hash_name].arg
            )

            query += " AND \"{}\"={}".format(
                self.SYSTEM_COLUMN_HASH, hash_value)

        #add limit
        if limit:
            query += " LIMIT {}".format(limit)

        #add ordering
        if order_type and range_name:
            query += " ORDER BY \"{}\" {}".format(
                self.USER_COLUMN_PREFIX + range_name, order_type
            )

        #add allow filtering
        if add_filtering:
            query += " ALLOW FILTERING"

        rows = self._execute_query(query, consistent)

        if select_type.is_count:
            return models.SelectResult(count=rows[0]['count'])

        # process results

        prefix_len = len(self.USER_COLUMN_PREFIX)
        attr_defs = {attr.name: attr.type for attr in schema.attribute_defs}
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
                        storage_type = attr_defs[name]
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

    def _fixed_range_condition(self, range_name,
                               condition_map, exclusive_start_key):

        if range_name not in exclusive_start_key:
            return None

        right_cond = None

        if range_name in condition_map:
            condition = condition_map.pop(range_name)

            if condition.type == models.Condition.CONDITION_TYPE_EQUAL:
                condition_map[range_name] = condition
                return None

            elif condition.type in (
                models.IndexedCondition.CONDITION_TYPE_LESS,
                models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL
            ):

                right_cond = condition

            elif condition.type == (models.IndexedCondition.
                                    CONDITION_TYPE_BETWEEN):

                first, second = condition.arg

                right_cond = models.IndexedCondition.le(second)

            elif condition.type == (models.IndexedCondition.
                                    CONDITION_TYPE_BEGINS_WITH):

                first = condition.arg
                second_value = first.value[:-1] + chr(
                    ord(first.value[-1]) + 1)
                second = models.AttributeValue(
                    condition.arg.type, second_value)

                right_cond = models.IndexedCondition.lt(second)

        left_cond = models.IndexedCondition.gt(
            exclusive_start_key[range_name])

        fixed = self._condition_as_string(range_name, left_cond)

        if right_cond:
            fixed += ' AND ' + self._condition_as_string(
                range_name, right_cond)

        return fixed

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

    @staticmethod
    def _add_filtering_and_sys_hash(schema, condition_map={}):

        condition_map = condition_map or {}

        hash_name = schema.key_attributes[0]

        if hash_name in condition_map:
            assert (condition_map[hash_name].type
                    == models.Condition.CONDITION_TYPE_EQUAL)

        try:
            range_name = schema.key_attributes[1]
        except IndexError:
            range_name = None

        non_pk_attrs = [
            key
            for key in condition_map.iterkeys()
            if key != hash_name and key != range_name
        ]

        non_pk_attrs_count = len(non_pk_attrs)

        if non_pk_attrs_count == 0:
            return False, False

        indexed_attrs = [
            ind_def.attribute_to_index
            for ind_def in schema.index_defs
        ]

        has_one_indexed_eq = any(
            [(attr in indexed_attrs) and
             (condition_map[attr].type ==
              models.Condition.CONDITION_TYPE_EQUAL)
             for attr in non_pk_attrs])

        add_sys_hash = not has_one_indexed_eq
        add_filtering = non_pk_attrs_count > 1 or add_sys_hash

        return add_filtering, add_sys_hash

    def _conditions_satisfied(self, row, cond_map={}):
        cond_map = cond_map or {}
        return all([self._condition_satisfied(row.get(attr, None), cond)
                    for attr, cond in cond_map.iteritems()])

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

        if cond.type == models.IndexedCondition.CONDITION_TYPE_BETWEEN:
            first, second = cond.arg
            return (attr_val.type == first.type and
                    second.type == first.type and
                    first.value <= attr_val.value <= second.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_BEGINS_WITH:
            return (attr_val.type == cond.arg.type and
                    attr_val.value.startswith(cond.arg.value))

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
