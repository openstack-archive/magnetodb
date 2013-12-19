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

from cassandra import cluster
from cassandra import decoder

from magnetodb.common.exception import BackendInteractionException
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

    __schemas = {}

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
                 max_schema_agreement_wait=10):

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
            max_schema_agreement_wait=max_schema_agreement_wait
        )

        self.session = self.cluster.connect()
        self.session.row_factory = decoder.dict_factory

    def _execute_query(self, query):
        try:
            LOG.debug("Executing query {}".format(query))
            return self.session.execute(query)
        except Exception as e:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.error(msg)
            raise BackendInteractionException(
                msg)

    def create_table(self, context, table_schema):
        """
        Creates table

        @param context: current request context
        @param table_schema: TableSchema instance which define table to create

        @raise BackendInteractionException
        """

        query = "CREATE TABLE {}.{} (".format(context.tenant,
                                              table_schema.table_name)

        for attr_def in table_schema.attribute_defs:
            query += "{} {},".format(
                self.USER_COLUMN_PREFIX + attr_def.name,
                self.STORAGE_TO_CASSANDRA_TYPES[attr_def.type])

        query += "{} map<text, blob>,".format(self.SYSTEM_COLUMN_ATTRS)
        query += "{} map<text, text>,".format(self.SYSTEM_COLUMN_ATTR_TYPES)
        query += "{} set<text>,".format(self.SYSTEM_COLUMN_ATTR_EXIST)

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

        primary_key = ','.join(prefixed_attrs)
        query += "PRIMARY KEY ({})".format(primary_key)

        query += ")"

        try:
            self._execute_query(query)

            for index_def in table_schema.index_defs:
                self._create_index(context, table_schema.table_name,
                                   self.USER_COLUMN_PREFIX +
                                   index_def.attribute_to_index,
                                   index_def.index_name)

            self._create_index(
                context, table_schema.table_name, self.SYSTEM_COLUMN_HASH,
                self.SYSTEM_COLUMN_HASH_INDEX_NAME)

        except Exception as e:
            LOG.error("Table {} creation failed. Cleaning up...".format(
                table_schema.table_name))

            try:
                self.delete_table(context, table_schema.table_name)
            except Exception:
                LOG.error("Failed table {} was not deleted".format(
                    table_schema.table_name))

            raise e

    def _create_index(self, context, table_name, indexed_attr, index_name=""):
        if index_name:
            index_name = "_".join((table_name, index_name))

        query = "CREATE INDEX {} ON {}.{} ({})".format(
            index_name, context.tenant, table_name, indexed_attr)

        self._execute_query(query)

    def delete_table(self, context, table_name):
        """
        Creates table

        @param context: current request context
        @param table_name: String, name of table to delete

        @raise BackendInteractionException
        """
        query = "DROP TABLE {}.{}".format(context.tenant, table_name)

        self._execute_query(query)

    def describe_table(self, context, table_name):
        """
        Describes table

        @param context: current request context
        @param table_name: String, name of table to describes

        @return: TableSchema instance

        @raise BackendInteractionException
        """

        try:
            ks = self.__schemas[context.tenant]
        except KeyError:
            self.__schemas[context.tenant] = {}
            ks = {}

        try:
            return ks[table_name]
        except KeyError:
            # just proceed with schema retrieval
            pass

        try:
            keyspace_meta = self.cluster.metadata.keyspaces[context.tenant]
        except KeyError:
            raise BackendInteractionException(
                "Tenant '{}' does not exist".format(context.tenant))

        try:
            table_meta = keyspace_meta.tables[table_name]
        except KeyError:
            raise BackendInteractionException(
                "Table '{}' does not exist".format(table_name))

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

        self.__schemas[context.tenant][table_name] = table_schema

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

        query = "SELECT columnfamily_name from system.schema_columnfamilies"

        query += " WHERE keyspace_name = '{}'".format(context.tenant)

        if exclusive_start_table_name:
            query += " AND columnfamily_name > '{}'".format(
                exclusive_start_table_name)

        if limit:
            query += " LIMIT {}".format(limit)

        tables = self._execute_query(query)

        return [row['columnfamily_name'] for row in tables]

    def _indexed_attrs(self, context, table):
        schema = self.describe_table(context, table)
        return schema.indexed_attrs

    def _predefined_attrs(self, context, table):
        schema = self.describe_table(context, table)
        return [attr.name for attr in schema.attribute_defs]

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
                    set_clause += '{}{} = {},'.format(
                        self.USER_COLUMN_PREFIX,
                        attr, self._encode_value(val, True))
                elif attr in unset_attrs:
                    set_clause += '{}{} = null,'.format(
                        self.USER_COLUMN_PREFIX,  attr)

            set_clause += '{} = {{{}}},'.format(
                self.SYSTEM_COLUMN_ATTRS, dynamic_values
            )

            set_clause += '{} = {{{}}},'.format(
                self.SYSTEM_COLUMN_ATTR_TYPES, types
            )

            set_clause += '{} = {{{}}},'.format(
                self.SYSTEM_COLUMN_ATTR_EXIST, exists
            )

            set_clause += '{} = {}'.format(
                self.SYSTEM_COLUMN_HASH, hash_value
            )

            where = ' AND '.join((
                '{}{} = {}'.format(
                    self.USER_COLUMN_PREFIX,
                    attr, self._encode_value(val, True))
                for attr, val in attr_map.iteritems()
                if attr in key_attrs
            ))

            query = 'UPDATE {}.{} SET {} WHERE {}'.format(
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
                    attrs += '{}{},'.format(self.USER_COLUMN_PREFIX, attr)
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

            query = 'INSERT INTO {}.{} ({}) VALUES ({})'.format(
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
        query = "DELETE FROM {}.{} WHERE ".format(
            context.tenant, delete_request.table_name)

        where = self._conditions_as_string(delete_request.key_attribute_map)

        query += where

        if expected_condition_map:
            if_clause = self._conditions_as_string(expected_condition_map)

            query += " IF " + if_clause

        self._execute_query(query)

        return True

    def _condition_as_string(self, attr, condition_or_attr_value):
        name = self.USER_COLUMN_PREFIX + attr

        condition = (
            models.Condition.eq(condition_or_attr_value)
            if isinstance(condition_or_attr_value, models.AttributeValue) else
            condition_or_attr_value
        )

        if condition.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            if condition.arg:
                return "{}={{'{}'}}".format(
                    self.SYSTEM_COLUMN_ATTR_EXIST, attr)
            else:
                return name + '=null'
        else:
            op = self.CONDITION_TO_OP[condition.type]
            return name + op + self._encode_predefined_attr_value(
                condition.arg
            )

    def _conditions_as_string(self, condition_map):
        return " AND ".join((self._condition_as_string(attr, cond)
                             for attr, cond
                             in condition_map.iteritems()))

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
        schema = self.describe_table(context, table_name)
        set_clause = self._updates_as_string(
            schema, key_attribute_map, attribute_action_map)

        where = self._conditions_as_string(key_attribute_map)

        query = "UPDATE {}.{} SET {} WHERE {}".format(
            context.tenant, table_name, set_clause, where
        )

        if expected_condition_map:
            if_clause = self._conditions_as_string(expected_condition_map)
            query += " IF {}".format(if_clause)

        self._execute_query(query)

    def _updates_as_string(self, schema, key_attribute_map, update_map):
        predefined_attrs = [attr.name for attr in schema.attribute_defs]

        set_clause = ", ".join({
            self._update_as_string(attr, update, attr in predefined_attrs)
            for attr, update in update_map.iteritems()})

        #update system_hash
        hash_name = schema.key_attributes[0]
        hash_value = self._encode_predefined_attr_value(
            key_attribute_map[hash_name].arg
        )

        set_clause += ",{}={}".format(self.SYSTEM_COLUMN_HASH, hash_value)

        return set_clause

    def _update_as_string(self, attr, update, is_predefined):
        if is_predefined:
            name = self.USER_COLUMN_PREFIX + attr
        else:
            name = "{}['{}']".format(self.SYSTEM_COLUMN_ATTRS, attr)

        # delete value
        if (update.action == models.UpdateItemAction.UPDATE_ACTION_DELETE
            or (update.action == models.UpdateItemAction.UPDATE_ACTION_PUT
                and (not update.value or not update.value.value))):
            value = 'null'

            type_update = "{}['{}'] = null".format(
                self.SYSTEM_COLUMN_ATTR_TYPES, attr)

            exists = "{} = {} - {{'{}'}}".format(
                self.SYSTEM_COLUMN_ATTR_EXIST,
                self.SYSTEM_COLUMN_ATTR_EXIST, attr)
        # put or add
        else:
            type_update = "{}['{}'] = '{}'".format(
                self.SYSTEM_COLUMN_ATTR_TYPES, attr,
                self.STORAGE_TO_CASSANDRA_TYPES[update.value.type])

            exists = "{} = {} + {{'{}'}}".format(
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
            return "textAsBlob('{}')".format(value)
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
        return "textAsBlob('{}')".format(json.dumps(val))

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

    def _decode_value(self, value, storage_type, is_predefined):
        if not is_predefined:
            value = json.loads(value)

        if storage_type.collection_type:
            decoded = frozenset(map(
                lambda e: self._decode_single_value(
                    e, storage_type.element_type
                ),
                value
            ))
        else:
            decoded = self._decode_single_value(value,
                                                storage_type.element_type)

        return models.AttributeValue(storage_type, decoded)

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

    def select_item(self, context, table_name, indexed_condition_map,
                    select_type=None, limit=None, consistent=True,
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

        @param limit: maximum count of returned values
        @param consistent: define is operation consistent or not (by default it
                    is not consistent)
        @param order_type: defines order of returned rows

        @return list of attribute name to AttributeValue mappings

        @raise BackendInteractionException
        """

        select_type = select_type or models.SelectType.all()

        select = 'COUNT(*)' if select_type.is_count else '*'

        query = "SELECT {} FROM {}.{} WHERE ".format(
            select, context.tenant, table_name)

        where = self._conditions_as_string(indexed_condition_map)

        query += where

        # add system_hash condition
        schema = self.describe_table(context, table_name)
        hash_name = schema.key_attributes[0]
        try:
            hash_value = self._encode_predefined_attr_value(
                indexed_condition_map[hash_name].arg
            )

            query += " AND {}={}".format(
                self.SYSTEM_COLUMN_HASH, hash_value)
        except KeyError:
            # do nothing
            # just don't add condition on system_hash
            pass

        #add limit
        if limit:
            query += " LIMIT {}".format(limit)

        #add ordering
        try:
            range_name = schema.key_attributes[1]
        except IndexError:
            range_name = None

        if order_type and range_name:
            query += " ORDER BY {} {}".format(
                self.USER_COLUMN_PREFIX + range_name, order_type
            )

        query += " ALLOW FILTERING"

        if consistent:
            query = cluster.SimpleStatement(query)
            query.consistency_level = cluster.ConsistencyLevel.QUORUM

        rows = self._execute_query(query)

        if select_type.is_count:
            return [{'count': models.AttributeValue.number(rows[0]['count'])}]

        # process results

        prefix_len = len(self.USER_COLUMN_PREFIX)
        attr_defs = {attr.name: attr.type for attr in schema.attribute_defs}
        result = []

        attributes_to_get = select_type.attributes if select_type else None

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
                    type = types[name]
                    storage_type = self.CASSANDRA_TO_STORAGE_TYPES[type]
                    record[name] = self._decode_value(
                        val, storage_type, False)

            result.append(record)

        return result
