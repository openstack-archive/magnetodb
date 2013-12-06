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
import json

from magnetodb.common import config
from magnetodb.common.exception import BackendInteractionException
from magnetodb.openstack.common import log as logging
from magnetodb.storage.models import AttributeDefinition
from magnetodb.storage.models import AttributeType
from magnetodb.storage.models import TableSchema


LOG = logging.getLogger(__name__)
CONF = config.CONF

storage_param = json.loads(CONF.storage_param) if CONF.storage_param else {}

CLUSTER = cluster.Cluster(**storage_param)
SESSION = CLUSTER.connect()

STORAGE_TO_CASSANDRA_TYPES = {
    AttributeType.ELEMENT_TYPE_STRING: 'text',
    AttributeType.ELEMENT_TYPE_NUMBER: 'decimal',
    AttributeType.ELEMENT_TYPE_BLOB: 'blob'
}

CASSANDRA_TO_STORAGE_TYPES = {val: key for key, val
                              in STORAGE_TO_CASSANDRA_TYPES.iteritems()}

USER_COLUMN_PREFIX = 'user_'
SYSTEM_COLUMN_PREFIX = 'system_'
SYSTEM_COLUMN_ATTRS = SYSTEM_COLUMN_PREFIX + 'attrs'
SYSTEM_COLUMN_ATTR_TYPES = SYSTEM_COLUMN_PREFIX + 'attr_types'
SYSTEM_COLUMN_ATTR_EXISTS = SYSTEM_COLUMN_PREFIX + 'attr_exists'


def _execute_query(query):
    try:
        LOG.debug("Executing query {}".format(query))
        return SESSION.execute(query)
    except Exception as e:
        msg = "Error executing query {}:{}".format(query, e.message)
        LOG.error(msg)
        raise BackendInteractionException(
            msg)


def create_table(context, table_schema):
    """
    Creates table

    @param context: current request context
    @param table_schema: TableSchema instance which define table to create

    @raise BackendInteractionException
    """

    query = "CREATE TABLE {}.{} (".format(context.tenant,
                                          table_schema.table_name)

    for attr_def in table_schema.attribute_defs:
        query += "{} {},".format(USER_COLUMN_PREFIX + attr_def.name,
                                 STORAGE_TO_CASSANDRA_TYPES[attr_def.type])

    query += "{} map<text, blob>,".format(SYSTEM_COLUMN_ATTRS)
    query += "{} map<text, text>,".format(SYSTEM_COLUMN_ATTR_TYPES)
    query += "{} map<text, text>,".format(SYSTEM_COLUMN_ATTR_EXISTS)

    prefixed_attrs = [USER_COLUMN_PREFIX + name
                      for name in table_schema.key_attributes]

    key_count = len(prefixed_attrs)

    if key_count < 1 or key_count > 2:
        raise BackendInteractionException(
            "Expected 1 or 2 key attribute(s). Found {}: {}".format(
                key_count, table_schema.key_attributes))

    primary_key = ','.join(prefixed_attrs)
    query += "PRIMARY KEY ({})".format(primary_key)

    query += ")"

    try:
        _execute_query(query)

        for attr in table_schema.indexed_non_key_attributes:
            _create_index(context, table_schema.table_name,
                          USER_COLUMN_PREFIX + attr)
    except Exception as e:
        LOG.error("Table {} creation failed. Cleaning up...".format(
            table_schema.table_name))

        try:
            delete_table(context, table_schema.table_name)
        except Exception:
            LOG.error("Failed table {} was not deleted".format(
                table_schema.table_name))

        raise e


def _create_index(context, table_name, indexed_attr):

    query = "CREATE INDEX ON {}.{} ({})".format(
        context.tenant, table_name, indexed_attr)

    _execute_query(query)


def delete_table(context, table_name):
    """
    Creates table

    @param context: current request context
    @param table_name: String, name of table to delete

    @raise BackendInteractionException
    """
    query = "DROP TABLE {}.{}".format(context.tenant, table_name)

    _execute_query(query)


def describe_table(context, table_name):
    """
    Creates table

    @param context: current request context
    @param table_name: String, name of table to describes

    @return: TableSchema instance

    @raise BackendInteractionException
    """
    try:
        keyspace_meta = CLUSTER.metadata.keyspaces[context.tenant]
    except KeyError:
        raise BackendInteractionException(
            "Tenant '{}' does not exist".format(context.tenant))

    try:
        table_meta = keyspace_meta.tables[table_name]
    except KeyError:
        raise BackendInteractionException(
            "Table '{}' does not exist".format(table_name))

    prefix_len = len(USER_COLUMN_PREFIX)

    user_columns = [val for key, val
                    in table_meta.columns.iteritems()
                    if key.startswith(USER_COLUMN_PREFIX)]

    attr_defs = []
    indexed_attrs = []

    for column in user_columns:
        name = column.name[prefix_len:]
        type = CASSANDRA_TO_STORAGE_TYPES[column.typestring]
        attr_defs.append(AttributeDefinition(name, type))
        if column.index:
            indexed_attrs.append(name)

    hash_key_name = table_meta.partition_key[0].name[prefix_len:]

    key_attrs = [hash_key_name]

    if table_meta.clustering_key:
        range_key_name = table_meta.clustering_key[0].name[prefix_len:]
        key_attrs.append(range_key_name)

    table_schema = TableSchema(table_meta.name, attr_defs,
                               key_attrs, indexed_attrs)

    return table_schema


def list_tables(context, exclusive_start_table_name=None, limit=None):
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

    tables = _execute_query(query)

    return [row.columnfamily_name for row in tables]


def put_item(context, put_request, if_not_exist=False,
             expected_condition_map=None):
    """
    @param context: current request context
    @param put_request: contains PutItemRequest items to perform
                put item operation
    @param if_not_exist: put item only is row is new record (It is possible to
                use only one of if_not_exist and expected_condition_map
                parameter)
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides preconditions
                to make decision about should item be put or not

    @return: True if operation performed, otherwise False

    @raise BackendInteractionException
    """
    raise NotImplemented


def delete_item(context, delete_request, expected_condition_map=None):
    """
    @param context: current request context
    @param delete_request: contains DeleteItemRequest items to perform
                delete item operation
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides preconditions
                to make decision about should item be deleted or not

    @return: True if operation performed, otherwise False (if operation was
                skipped by out of date timestamp, it is considered as
                successfully performed)

    @raise BackendInteractionException
    """
    raise NotImplemented


def execute_write_batch(context, write_request_list, durable=True):
    """
    @param context: current request context
    @param write_request_list: contains WriteItemBatchableRequest items to
                perform batch
    @param durable: if True, batch will be fully performed or fully skipped.
                Partial batch execution isn't allowed

    @raise BackendInteractionException
    """
    raise NotImplemented


def update_item(context, table_name, key_attribute_map, attribute_action_map,
                expected_condition_map=None):
    """
    @param context: current request context
    @param table_name: String, name of table to delete item from
    @param key_attribute_map: key attribute name to
                AttributeValue mapping. It defines row it to update item
    @param attribute_action_map: attribute name to UpdateItemAction instance
                mapping. It defines actions to perform for each given attribute
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides preconditions
                to make decision about should item be updated or not
    @return: True if operation performed, otherwise False

    @raise BackendInteractionException
    """
    raise NotImplemented


def select_item(context, table_name, indexed_condition_map,
                attributes_to_get=None, limit=None, consistent=True):
    """
    @param context: current request context
    @param table_name: String, name of table to get item from
    @param indexed_condition_map: indexed attribute name to
                IndexedCondition instance mapping. It defines rows
                set to be selected
    @param attributes_to_get: attribute name list to get. If not specified, all
                attributes should be returned. Also aggregate functions are
                allowed, if they are supported by storage implementation

    @param limit: maximum count of returned values
    @param consistent: define is operation consistent or not (by default it is
                not consistent)

    @return list of attribute name to AttributeValue mappings

    @raise BackendInteractionException
    """
    raise NotImplemented
