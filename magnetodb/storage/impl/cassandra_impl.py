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

from cassandra import cluster
from cassandra import decoder

from magnetodb.common.exception import BackendInteractionException
from magnetodb.openstack.common import log as logging
from magnetodb.storage import models
from magnetodb.storage.models import IndexDefinition

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
                index_defs.add(IndexDefinition(
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

    def _external_attrs(self, context, table):
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
        raise NotImplementedError

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

        where = " AND ".join((self._condition_as_string(attr, cond)
                              for attr, cond
                              in delete_request.key_attribute_map.iteritems()))

        query += where

        if expected_condition_map:
            if_clause = " AND ".join((self._condition_as_string(attr, cond)
                                      for attr, cond
                                      in expected_condition_map.iteritems()))

            query += " IF " + if_clause

        self._execute_query(query)

        return True

    def _condition_as_string(self, attr, condition):
        name = self.USER_COLUMN_PREFIX + attr

        if condition.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            if condition.arg:
                return "{}={{'{}'}}".format(
                    self.SYSTEM_COLUMN_ATTR_EXIST, attr)
            else:
                return name + '=null'
        else:
            op = self.CONDITION_TO_OP[condition.type]
            return name + op + repr(condition.arg)

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
        raise NotImplementedError

    def select_item(self, context, table_name, indexed_condition_map,
                    attributes_to_get=None, limit=None, consistent=True,
                    order_type=None):
        """
        @param context: current request context
        @param table_name: String, name of table to get item from
        @param indexed_condition_map: indexed attribute name to
                    IndexedCondition instance mapping. It defines rows
                    set to be selected
        @param attributes_to_get: attribute name list to get. If not specified,
                    all attributes should be returned. Also aggregate functions
                    are allowed, if they are supported by storage
                    implementation

        @param limit: maximum count of returned values
        @param consistent: define is operation consistent or not (by default it
                    is not consistent)
        @param order_type: defines order of returned rows

        @return list of attribute name to AttributeValue mappings

        @raise BackendInteractionException
        """

        schema = self.describe_table(context, table_name)
        attr_defs = {attr.name: attr.type for attr in schema.attribute_defs}

        query = "SELECT * FROM {}.{} WHERE ".format(
            context.tenant, table_name)

        where = " AND ".join((self._condition_as_string(attr, cond)
                              for attr, cond
                              in indexed_condition_map.iteritems()))

        query += where

        hash_name = schema.key_attributes[0]

        try:
            hash_value = indexed_condition_map[hash_name].arg

            query += " AND {}={}".format(
                self.SYSTEM_COLUMN_HASH, hash_value)
        except KeyError:
            # do nothing
            # just don't add condition on system_hash
            pass

        if limit:
            query += " LIMIT {}".format(limit)

        try:
            range_name = schema.key_attributes[1]
        except IndexError:
            range_name = None

        if order_type and range_name:
            query += " ORDER BY {} {}".format(
                self.USER_COLUMN_PREFIX + range_name, order_type
            )

        query += " ALLOW FILTERING"

        result = []

        if consistent:
            query = cluster.SimpleStatement(query)
            query.consistency_level = cluster.ConsistencyLevel.QUORUM

        rows = self._execute_query(query)

        prefix_len = len(self.USER_COLUMN_PREFIX)

        for row in rows:
            record = {}

            for key, val in row.iteritems():
                if key.startswith(self.USER_COLUMN_PREFIX):
                    name = key[prefix_len:]
                    if not attributes_to_get or name in attributes_to_get:
                        storage_type = attr_defs[name]
                        record[name] = models.AttributeValue(storage_type, val)

            types = row[self.SYSTEM_COLUMN_ATTR_TYPES]

            for name, val in row[self.SYSTEM_COLUMN_ATTRS].iteritems():
                if not attributes_to_get or name in attributes_to_get:
                    type = types[name]
                    storage_type = self.CASSANDRA_TO_STORAGE_TYPES[type]
                    record[name] = models.AttributeValue(storage_type, val)

            result.append(record)

        return result
