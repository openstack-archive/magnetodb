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

from cassandra import query, ConsistencyLevel
from cassandra.query import SimpleStatement
from magnetodb.common.cassandra.cluster import Cluster
from magnetodb.common.cassandra.cluster import NoHostAvailable

from magnetodb.common.exception import BackendInteractionException
from magnetodb.common.exception import TableNotExistsException
from magnetodb.common.exception import TableAlreadyExistsException
from magnetodb.openstack.common import importutils
from magnetodb.openstack.common import log as logging
from magnetodb.storage import models

LOG = logging.getLogger(__name__)

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
USER_PREFIX_LENGTH = len(USER_PREFIX)

SYSTEM_KEYSPACE = 'magnetodb'
SYSTEM_COLUMN_INDEX_NAME = 'index_name'
SYSTEM_COLUMN_INDEX_VALUE_STRING = 'index_value_string'
SYSTEM_COLUMN_INDEX_VALUE_NUMBER = 'index_value_number'
SYSTEM_COLUMN_INDEX_VALUE_BLOB = 'index_value_blob'

LOCAL_INDEX_FIELD_LIST = [
    SYSTEM_COLUMN_INDEX_NAME,
    SYSTEM_COLUMN_INDEX_VALUE_STRING,
    SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
    SYSTEM_COLUMN_INDEX_VALUE_BLOB
]

INDEX_TYPE_TO_INDEX_POS_MAP = {
    models.ATTRIBUTE_TYPE_STRING: 1,
    models.ATTRIBUTE_TYPE_NUMBER: 2,
    models.ATTRIBUTE_TYPE_BLOB: 3,
}

SYSTEM_COLUMN_EXTRA_ATTR_DATA = 'extra_attr_data'
SYSTEM_COLUMN_EXTRA_ATTR_TYPES = 'extra_attr_types'
SYSTEM_COLUMN_ATTR_EXIST = 'attr_exist'

DEFAULT_STRING_VALUE = models.AttributeValue.str('')
DEFAULT_NUMBER_VALUE = models.AttributeValue.number(0)
DEFAULT_BLOB_VALUE = models.AttributeValue.blob('')


def _encode_predefined_attr_value(attr_value):
    if attr_value is None:
        return 'null'
    if attr_value.type.collection_type:
        values = ','.join(map(
            lambda el: _encode_single_value_as_predefined_attr(
                el, attr_value.type.element_type),
            attr_value.value
        ))
        return '{{{}}}'.format(values)
    else:
        return _encode_single_value_as_predefined_attr(
            attr_value.value, attr_value.type.element_type
        )


def _encode_single_value_as_predefined_attr(value, element_type):
    if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
        return "'{}'".format(value)
    elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
        return str(value)
    elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
        return "0x{}".format(binascii.hexlify(value))
    else:
        assert False, "Value wasn't formatted for cql query"


def _encode_dynamic_attr_value(attr_value):
    if attr_value is None:
        return 'null'

    val = attr_value.value
    if attr_value.type.collection_type:
        val = map(
            lambda el: _encode_single_value_as_dynamic_attr(
                el, attr_value.type.element_type
            ),
            val
        )
        val.sort()
    else:
        val = _encode_single_value_as_dynamic_attr(
            val, attr_value.type.element_type)
    return "0x{}".format(binascii.hexlify(json.dumps(val)))


def _encode_single_value_as_dynamic_attr(value, element_type):
    if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
        return value
    elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
        return str(value)
    elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
        return value
    else:
        assert False, "Value wasn't formatted for cql query"


def _decode_predefined_attr(table_info, cas_name, cas_val, prefix=USER_PREFIX):
    assert cas_name.startswith(prefix) and cas_val

    name = cas_name[len(USER_PREFIX):]
    storage_type = table_info.schema.attribute_type_map[name]
    return name, models.AttributeValue(storage_type, cas_val)


def _decode_dynamic_value(value, storage_type):
    value = json.loads(value)

    return models.AttributeValue(storage_type, value)


def _decode_single_value(value, element_type):
    if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
        return value
    elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
        return Decimal(value)
    elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
        return value
    else:
        assert False, "Value wasn't formatted for cql query"

ENCODED_DEFAULT_STRING_VALUE = _encode_predefined_attr_value(
    DEFAULT_STRING_VALUE
)
ENCODED_DEFAULT_NUMBER_VALUE = _encode_predefined_attr_value(
    DEFAULT_NUMBER_VALUE
)
ENCODED_DEFAULT_BLOB_VALUE = _encode_predefined_attr_value(
    DEFAULT_BLOB_VALUE
)


class CassandraStorageImpl(object):
    __table_info_cache = {}

    @classmethod
    def _save_table_info_to_cache(cls, tenant, table_name, table_info):
        tenant_tables_cache = cls.__table_info_cache.get(tenant)
        if tenant_tables_cache is None:
            tenant_tables_cache = {}
            cls.__table_info_cache[tenant] = tenant_tables_cache
        tenant_tables_cache[table_name] = table_info

    @classmethod
    def _get_table_info_from_cache(cls, tenant, table_name):
        tenant_tables_cache = cls.__table_info_cache.get(tenant)
        if tenant_tables_cache is None:
            return None
        return tenant_tables_cache.get(table_name)

    @classmethod
    def _remove_table_schema_from_cache(cls, tenant, table_name):
        tenant_tables_cache = cls.__table_info_cache.get(tenant)
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

        self.cluster = Cluster(
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
        self.session.row_factory = query.dict_factory
        self.session.default_timeout = query_timeout

    def schema_change_listener(self, event):
        LOG.debug("Schema change event captured: %s" % event)

        keyspace = event.get('keyspace')
        table_name = event.get('table')

        if (keyspace is None) or (table_name is None):
            return

        tenant = keyspace[USER_PREFIX_LENGTH:]
        table_name = table_name[USER_PREFIX_LENGTH:]

        if event['change_type'] == "DROPPED":
            self._remove_table_schema_from_cache(
                tenant, table_name)

    def _execute_query(self, query, consistent=False):
        ex = None
        if consistent:
            query = SimpleStatement(
                query, consistency_level=ConsistencyLevel.QUORUM
            )
        LOG.debug("Executing query {}".format(query))
        for x in range(3):
            try:
                return self.session.execute(query)
            except NoHostAvailable:
                LOG.warning("It seems connection was lost. Retrying...",
                            exc_info=1)
            except Exception as e:
                ex = e
                break
        if ex:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.exception(msg)
            raise BackendInteractionException(msg)

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

    def create_table(self, context, table_name, table_schema):
        """
        Creates table

        @param context: current request context
        @param table_name: String, name of the table to create
        @param table_schema: TableSchema instance which define table to create

        @return TableMeta instance with metadata of created table

        @raise BackendInteractionException
        """

        try:
            table_info = TableInfo(
                self, context.tenant, table_name, table_schema,
                models.TableMeta.TABLE_STATUS_CREATING
            )

            res = table_info.save()

            if not res:
                raise TableAlreadyExistsException(
                    "Table '{}' already exists".format(table_name)
                )

            internal_table_name = self._do_create_table(
                context, table_name, table_schema
            )

            table_info.internal_name = internal_table_name
            table_info.status = models.TableMeta.TABLE_STATUS_ACTIVE
            res = table_info.update("internal_name", "status")

            if not res:
                raise BackendInteractionException("Can't update table status")
        except Exception as e:
            LOG.exception("Table {} creation failed.".format(table_name))

            raise e

        return models.TableMeta(table_info.schema, table_info.status)

    def _do_create_table(self, context, table_name, table_schema):
        cas_table_name = USER_PREFIX + table_name
        cas_keyspace = USER_PREFIX + context.tenant
        key_count = len(table_schema.key_attributes)

        if key_count < 1 or key_count > 2:
            raise BackendInteractionException(
                "Expected 1 or 2 key attribute(s). Found {}: {}".format(
                    key_count, table_schema.key_attributes))

        hash_key_name = table_schema.key_attributes[0]
        range_key_name = (
            table_schema.key_attributes[1] if key_count > 1 else None
        )

        query_builder = [
            'CREATE TABLE "', cas_keyspace, '"."', cas_table_name, '" ('
        ]

        if table_schema.index_def_map:
            query_builder += (
                SYSTEM_COLUMN_INDEX_NAME, " text,",
                SYSTEM_COLUMN_INDEX_VALUE_STRING, " text, ",
                SYSTEM_COLUMN_INDEX_VALUE_NUMBER, " decimal, ",
                SYSTEM_COLUMN_INDEX_VALUE_BLOB, " blob, ",
            )

        for attr_name, attr_type in (
                table_schema.attribute_type_map.iteritems()):

            query_builder += (
                '"', USER_PREFIX, attr_name, '" ',
                STORAGE_TO_CASSANDRA_TYPES[attr_type], ","
            )

        query_builder += (
            SYSTEM_COLUMN_EXTRA_ATTR_DATA, " map<text, blob>,",
            SYSTEM_COLUMN_EXTRA_ATTR_TYPES, " map<text, text>,",
            SYSTEM_COLUMN_ATTR_EXIST, " set<text>,"
            'PRIMARY KEY ("',
            USER_PREFIX, hash_key_name, '"'
        )

        if table_schema.index_def_map:
            query_builder += (
                ",", SYSTEM_COLUMN_INDEX_NAME,
                ",", SYSTEM_COLUMN_INDEX_VALUE_STRING,
                ",", SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                ",", SYSTEM_COLUMN_INDEX_VALUE_BLOB,
            )

        if range_key_name:
            query_builder += (
                ',"', USER_PREFIX, range_key_name, '"'
            )

        query_builder.append("))")

        self._execute_query("".join(query_builder))

        LOG.debug("Create Table CQL request executed. "
                  "Waiting for schema agreement...")

        self._wait_for_table_status(keyspace_name=cas_keyspace,
                                    table_name=cas_table_name,
                                    expected_exists=True)
        LOG.debug("Waiting for schema agreement... Done")

        return cas_table_name

    def delete_table(self, context, table_name):
        """
        Creates table

        @param context: current request context
        @param table_name: String, name of table to delete

        @raise BackendInteractionException
        """
        table_info = self._get_table_info(context, table_name)

        if table_info is None:
            raise TableNotExistsException(
                "Table '{}' does not exists".format(table_name)
            )

        table_info.status = models.TableMeta.TABLE_STATUS_DELETING
        table_info.update("status")

        cas_table_name = table_info.internal_name
        cas_keyspace_name = USER_PREFIX + context.tenant

        query = 'DROP TABLE "{}"."{}"'.format(cas_keyspace_name,
                                              cas_table_name)

        self._execute_query(query)

        LOG.debug("Delete Table CQL request executed. "
                  "Waiting for schema agreement...")

        self._wait_for_table_status(keyspace_name=cas_keyspace_name,
                                    table_name=cas_table_name,
                                    expected_exists=False)

        table_info.delete()

        LOG.debug("Waiting for schema agreement... Done")

    def _get_table_info(self, context, table_name):
        """
        Describes table

        @param context: current request context
        @param table_name: String, name of table to describes

        @return: TableSchema instance

        @raise BackendInteractionException
        """

        table_info = self._get_table_info_from_cache(context.tenant,
                                                     table_name)
        if table_info:
            return table_info

        table_info = TableInfo.load(self, context.tenant, table_name)

        if table_info is None:
            return None

        self._save_table_info_to_cache(context.tenant, table_name, table_info)
        return table_info

    def describe_table(self, context, table_name):
        """
        Describes table

        @param context: current request context
        @param table_name: String, name of table to describes

        @return: TableSchema instance

        @raise BackendInteractionException
        """

        table_info = self._get_table_info(context, table_name)
        table_info.refresh("status")

        return models.TableMeta(table_info.schema, table_info.status)

    def list_tables(self, context, exclusive_start_table_name=None,
                    limit=None):
        """
        @param context: current request context
        @param exclusive_start_table_name
        @param limit: limit of returned table names
        @return list of table names

        @raise BackendInteractionException
        """
        return TableInfo.load_tenant_table_names(
            self, context.tenant, exclusive_start_table_name, limit
        )

    @staticmethod
    def _append_types_system_attr_value(table_schema, attribute_map,
                                        query_builder=None, prefix=""):
        if query_builder is None:
            query_builder = []
        query_builder.append(prefix)
        prefix = ""
        query_builder.append("{")
        for attr, val in attribute_map.iteritems():
            if (val is not None) and (
                    attr not in table_schema.attribute_type_map):
                query_builder += (
                    prefix, "'", attr, "':'",
                    STORAGE_TO_CASSANDRA_TYPES[val.type], "'"
                )
                prefix = ","
        query_builder.append("}")
        return query_builder

    @staticmethod
    def _append_exists_system_attr_value(attribute_map, query_builder=None,
                                         prefix=""):
        if query_builder is None:
            query_builder = []
        query_builder.append(prefix)
        prefix = ""
        query_builder.append("{")
        for attr, _ in attribute_map.iteritems():
            query_builder += (prefix, "'", attr, "'")
            prefix = ","
        query_builder.append("}")
        return query_builder

    def _append_insert_query(
            self, table_info, attribute_map, query_builder=None,
            index_name=None, index_value=None, if_not_exists=False):
        if query_builder is None:
            query_builder = []

        query_builder += (
            'INSERT INTO "', USER_PREFIX, table_info.tenant, '"."',
            table_info.internal_name, '" ('
        )
        attr_values = []
        dynamic_attr_names = []
        dynamic_attr_values = []
        for name, val in attribute_map.iteritems():
            if name in table_info.schema.attribute_type_map.keys():
                query_builder += (
                    '"', USER_PREFIX, name, '",'
                )
                attr_values.append(_encode_predefined_attr_value(val))
            else:
                dynamic_attr_names.append(name)
                dynamic_attr_values.append(
                    _encode_dynamic_attr_value(val)
                )

        if table_info.schema.index_def_map:
            query_builder += (
                SYSTEM_COLUMN_INDEX_NAME, ",",
                SYSTEM_COLUMN_INDEX_VALUE_STRING, ",",
                SYSTEM_COLUMN_INDEX_VALUE_NUMBER, ",",
                SYSTEM_COLUMN_INDEX_VALUE_BLOB, ",",
            )

        query_builder += (
            SYSTEM_COLUMN_EXTRA_ATTR_DATA, ",",
            SYSTEM_COLUMN_EXTRA_ATTR_TYPES, ",",
            SYSTEM_COLUMN_ATTR_EXIST,
            ") VALUES("
        )

        for attr_value in attr_values:
            query_builder += (
                attr_value, ","
            )

        if table_info.schema.index_def_map:
            res_index_values = [
                ENCODED_DEFAULT_STRING_VALUE,
                ENCODED_DEFAULT_STRING_VALUE,
                ENCODED_DEFAULT_NUMBER_VALUE,
                ENCODED_DEFAULT_BLOB_VALUE
            ]

            if index_name:
                res_index_values[0] = _encode_single_value_as_predefined_attr(
                    index_name, models.AttributeType.ELEMENT_TYPE_STRING
                )

                res_index_values[INDEX_TYPE_TO_INDEX_POS_MAP[
                    index_value.type]
                ] = _encode_predefined_attr_value(index_value)

            for value in res_index_values:
                query_builder += (
                    value, ","
                )

        query_builder.append("{")

        if dynamic_attr_values:
            dynamic_value_iter = iter(dynamic_attr_values)
            for name in dynamic_attr_names:
                query_builder += (
                    "'", name, "':" + dynamic_value_iter.next(), ","
                )
            query_builder.pop()

        query_builder.append("},")
        self._append_types_system_attr_value(table_info.schema, attribute_map,
                                             query_builder)
        self._append_exists_system_attr_value(attribute_map, query_builder,
                                              prefix=",")
        query_builder.append(")")

        if if_not_exists:
            query_builder.append(" IF NOT EXISTS")
        return query_builder

    def _append_update_query_with_basic_pk(self, table_info, attribute_map,
                                           query_builder=None, rewrite=False):
        if query_builder is None:
            query_builder = []

        key_attr_names = table_info.schema.key_attributes

        not_processed_predefined_attr_names = set(
            table_info.schema.attribute_type_map.keys()
        )

        query_builder += (
            'UPDATE "', USER_PREFIX, table_info.tenant, '"."',
            table_info.internal_name, '" SET '
        )

        dynamic_attrs_to_set = []
        dynamic_attrs_to_delete = []

        set_prefix = ""

        for name, val in attribute_map.iteritems():
            if name in key_attr_names:
                not_processed_predefined_attr_names.remove(name)
            elif name in not_processed_predefined_attr_names:
                query_builder += (
                    set_prefix, '"', USER_PREFIX, name, '"=',
                    _encode_predefined_attr_value(val),
                )
                set_prefix = ","
                not_processed_predefined_attr_names.remove(name)
            else:
                if val is None:
                    dynamic_attrs_to_delete.append(name)
                else:
                    dynamic_attrs_to_set.append(
                        (name, _encode_dynamic_attr_value(val))
                    )

        if rewrite:
            query_builder += (set_prefix, SYSTEM_COLUMN_EXTRA_ATTR_DATA, "={")

            field_prefix = ""
            for name, value in dynamic_attrs_to_set:
                query_builder += (
                    field_prefix, "'", name, "':", value
                )
                field_prefix = ","
            query_builder.append("},")

            for name in not_processed_predefined_attr_names:
                query_builder += (
                    '"', USER_PREFIX, name, '"=null,'
                )

            query_builder.append(SYSTEM_COLUMN_EXTRA_ATTR_TYPES)
            self._append_types_system_attr_value(table_info.schema,
                                                 attribute_map, query_builder,
                                                 prefix="=")
            query_builder += (",", SYSTEM_COLUMN_ATTR_EXIST)
            self._append_exists_system_attr_value(attribute_map, query_builder,
                                                  prefix="=")
        else:
            if dynamic_attrs_to_set:
                query_builder += (
                    set_prefix, SYSTEM_COLUMN_EXTRA_ATTR_DATA, "=",
                    SYSTEM_COLUMN_EXTRA_ATTR_DATA, "+{"
                )
                set_prefix = ","

                field_prefix = ""
                for name, value in dynamic_attrs_to_set:
                    query_builder += (
                        field_prefix, "'", name, "':", value
                    )
                    field_prefix = ","
                query_builder.append("},")

                query_builder += (
                    SYSTEM_COLUMN_EXTRA_ATTR_TYPES, "=",
                    SYSTEM_COLUMN_EXTRA_ATTR_TYPES
                )
                self._append_types_system_attr_value(
                    table_info.schema, attribute_map, query_builder, prefix="+"
                )
                query_builder += (
                    ",", SYSTEM_COLUMN_ATTR_EXIST, "=",
                    SYSTEM_COLUMN_ATTR_EXIST
                )
                self._append_exists_system_attr_value(
                    attribute_map, query_builder, prefix="+"
                )
            if dynamic_attrs_to_delete:
                for name in dynamic_attrs_to_delete:
                    query_builder += (
                        set_prefix,
                        SYSTEM_COLUMN_EXTRA_ATTR_DATA, "['", name, "']=null,",
                        SYSTEM_COLUMN_EXTRA_ATTR_TYPES, "['", name, "']=null,",
                    )
                query_builder += (
                    SYSTEM_COLUMN_ATTR_EXIST, "=",
                    SYSTEM_COLUMN_ATTR_EXIST, "-{"
                )
                field_prefix = ""
                for name in dynamic_attrs_to_delete:
                    query_builder += (
                        field_prefix, "'", name, "'",
                    )
                    field_prefix = ","
                query_builder.append("}")
        self._append_primary_key(table_info.schema, attribute_map,
                                 query_builder)

        return query_builder

    def _append_update_query(self, table_info, attribute_map,
                             query_builder=None, index_name=None,
                             index_value=None, expected_condition_map=None,
                             rewrite=False):
        query_builder = self._append_update_query_with_basic_pk(
            table_info, attribute_map, query_builder, rewrite=rewrite
        )

        if table_info.schema.index_def_map:
            self._append_index_extra_primary_key(query_builder, index_name,
                                                 index_value, " AND ")

        if expected_condition_map:
            self._append_expected_conditions(
                expected_condition_map, table_info.schema,
                query_builder
            )

        return query_builder

    def _append_update_indexes_queries(self, table_info, old_attribute_map,
                                       attribute_map, query_builder=None,
                                       separator=" ", rewrite=False):
        if query_builder is None:
            query_builder = []
        base_update_query = None
        base_delete_query = None

        def create_base_update_query():
            base_query_builder = (
                self._append_update_query_with_basic_pk(
                    table_info, attribute_map, rewrite=rewrite
                )
            )
            return "".join(base_query_builder)

        def create_base_delete_query():
            base_query_builder = (
                self._append_delete_query_with_basic_pk(
                    table_info, attribute_map
                )
            )
            return "".join(base_query_builder)

        for index_name, index_def in (
                table_info.schema.index_def_map.iteritems()):
            new_index_value = attribute_map.get(
                index_def.attribute_to_index, None
            )
            old_index_value = old_attribute_map.get(
                index_def.attribute_to_index, None
            )
            if new_index_value:
                base_update_query = (
                    base_update_query or create_base_update_query()
                )
                query_builder += (separator, base_update_query)
                self._append_index_extra_primary_key(
                    query_builder, index_name, new_index_value,
                )
            if old_index_value and old_index_value != new_index_value:
                base_delete_query = (
                    base_delete_query or create_base_delete_query()
                )
                query_builder += (separator, base_delete_query)
                self._append_index_extra_primary_key(
                    query_builder, index_name, old_index_value,
                )
        return query_builder

    def _put_item_if_not_exists(self, table_info, attribute_map):
        query_builder = self._append_insert_query(
            table_info, attribute_map, if_not_exists=True
        )

        if table_info.schema.index_def_map:
            qb_len = len(query_builder)
            self._append_update_indexes_queries(
                table_info, {}, attribute_map, query_builder, rewrite=True
            )
            if len(query_builder) > qb_len:
                query_builder.insert(0, self._get_batch_begin_clause())
                query_builder.append(self._get_batch_apply_clause())

            result = self._execute_query(
                "".join(query_builder), consistent=True)

            return result[0]['[applied]']

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

        table_info = self._get_table_info(context, put_request.table_name)

        if if_not_exist:
            if expected_condition_map:
                raise BackendInteractionException(
                    "Specifying expected_condition_map and"
                    "if_not_exist is not allowed both"
                )
            return self._put_item_if_not_exists(table_info,
                                                put_request.attribute_map)
        elif table_info.schema.index_def_map:
            while True:
                old_indexes = self._select_current_index_values(
                    table_info, put_request.attribute_map
                )

                if old_indexes is None:
                    if self._put_item_if_not_exists(table_info,
                                                    put_request.attribute_map):
                        return True
                    else:
                        continue

                query_builder = self._append_update_query(
                    table_info, put_request.attribute_map,
                    expected_condition_map=expected_condition_map, rewrite=True
                )

                if_prefix = " AND " if expected_condition_map else " IF "
                for index_name, index_def in (
                        table_info.schema.index_def_map.iteritems()):
                    old_index_value = old_indexes.get(
                        index_def.attribute_to_index, None
                    )

                    query_builder += (
                        if_prefix, '"', USER_PREFIX,
                        index_def.attribute_to_index, '"=',
                        _encode_predefined_attr_value(old_index_value)
                        if old_index_value else "null"
                    )
                    if_prefix = " AND "

                qb_len = len(query_builder)

                self._append_update_indexes_queries(
                    table_info, old_indexes, put_request.attribute_map,
                    query_builder, rewrite=True
                )
                if len(query_builder) > qb_len:
                    query_builder.insert(0, self._get_batch_begin_clause())
                    query_builder.append(self._get_batch_apply_clause())
                result = self._execute_query("".join(query_builder),
                                             consistent=True)

                if result[0]['[applied]']:
                    return True

                for attr_name, attr_value in old_indexes.iteritems():
                    cas_name = USER_PREFIX + attr_name
                    (_, current_value) = _decode_predefined_attr(
                        table_info, cas_name, result[0][cas_name])
                    if current_value != attr_value:
                        # index consistency condition wasn't passed
                        break
                else:
                    # expected condition wasn't passed
                    return False
        elif expected_condition_map:
            query_builder = self._append_update_query(
                table_info, put_request.attribute_map,
                expected_condition_map=expected_condition_map, rewrite=True
            )
            result = self._execute_query("".join(query_builder),
                                         consistent=True)
            return result[0]['[applied]']
        else:
            query_builder = self._append_insert_query(
                table_info, put_request.attribute_map
            )
            self._execute_query("".join(query_builder), consistent=True)
            return True

    @classmethod
    def _append_delete_query_with_basic_pk(
            cls, table_info, attribute_map, query_builder=None):
        if query_builder is None:
            query_builder = []
        query_builder += (
            'DELETE FROM "', USER_PREFIX, table_info.tenant, '"."',
            table_info.internal_name, '"'
        )
        cls._append_primary_key(table_info.schema, attribute_map,
                                query_builder)
        return query_builder

    @classmethod
    def _append_delete_query(
            cls, table_info, attribute_map, query_builder=None,
            index_name=None, index_value=None, expected_condition_map=None):
        query_builder = cls._append_delete_query_with_basic_pk(
            table_info, attribute_map, query_builder)

        if table_info.schema.index_def_map:
            cls._append_index_extra_primary_key(query_builder, index_name,
                                                index_value)

        if expected_condition_map:
            cls._append_expected_conditions(
                expected_condition_map, table_info.schema,
                query_builder
            )

        return query_builder

    def _select_current_index_values(
            self, table_info, attribute_map):
        query_buider = ["SELECT "]
        prefix = ""
        for index_def in table_info.schema.index_def_map.itervalues():
            query_buider += (
                prefix, '"', USER_PREFIX, index_def.attribute_to_index, '"'
            )
            prefix = ","

        query_buider += (
            ' FROM "', USER_PREFIX, table_info.tenant, '"."',
            table_info.internal_name, '"'
        )

        self._append_primary_key(table_info.schema, attribute_map,
                                 query_buider)
        self._append_index_extra_primary_key(query_buider, prefix=" AND ")

        select_result = self._execute_query("".join(query_buider),
                                            consistent=False)
        if not select_result:
            return None

        assert len(select_result) == 1
        index_values = {}

        for cas_attr_name, cas_attr_value in select_result[0].iteritems():
            if cas_attr_value:
                attr_name = cas_attr_name[USER_PREFIX_LENGTH:]
                attr_type = table_info.schema.attribute_type_map[
                    attr_name
                ]
                index_values[attr_name] = (
                    models.AttributeValue(attr_type, cas_attr_value)
                )
        return index_values

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

        table_info = self._get_table_info(context, delete_request.table_name)
        delete_query = "".join(
            self._append_delete_query(
                table_info, delete_request.key_attribute_map,
                expected_condition_map=expected_condition_map
            )
        )

        if table_info.schema.index_def_map:
            while True:
                old_indexes = self._select_current_index_values(
                    table_info, delete_request.key_attribute_map
                )

                if old_indexes is None:
                    # Nothing to delete
                    return not expected_condition_map

                query_builder = [delete_query]
                if_prefix = " AND " if expected_condition_map else " IF "
                for index_name, index_def in (
                        table_info.schema.index_def_map.iteritems()):
                    index_value = old_indexes.get(
                        index_def.attribute_to_index, None
                    )
                    query_builder += (
                        if_prefix, '"', USER_PREFIX,
                        index_def.attribute_to_index, '"=',
                        _encode_predefined_attr_value(index_value)
                        if index_value
                        else "null"
                    )
                    if_prefix = " AND "

                qb_len = len(query_builder)

                self._append_update_indexes_queries(
                    table_info, old_indexes, delete_request.key_attribute_map,
                    query_builder
                )
                if len(query_builder) > qb_len:
                    query_builder.insert(0, self._get_batch_begin_clause())
                    query_builder.append(self._get_batch_apply_clause())
                result = self._execute_query("".join(query_builder),
                                             consistent=True)

                if result[0]['[applied]']:
                    return True

                for attr_name, attr_value in old_indexes.iteritems():
                    cas_name = USER_PREFIX + attr_name
                    (_, current_value) = _decode_predefined_attr(
                        table_info, cas_name, result[0][cas_name])
                    if current_value != attr_value:
                        # index consistency condition wasn't passed
                        break
                else:
                    # expected condition wasn't passed
                    return False
        else:
            result = self._execute_query(delete_query, consistent=True)
            return (result is None) or result[0]['[applied]']

    @staticmethod
    def _compact_indexed_condition(cond_list):
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
                                  column_prefix=USER_PREFIX):
        if query_builder is None:
            query_builder = []
        op = CONDITION_TO_OP[condition.type]
        query_builder += (
            '"', column_prefix, attr_name, '"', op,
            _encode_predefined_attr_value(condition.arg)
        )
        return query_builder

    def _append_hash_key_indexed_condition(
            self, attr_name, condition, query_builder,
            column_prefix=USER_PREFIX):
        if condition.type == models.IndexedCondition.CONDITION_TYPE_EQUAL:
            return self._append_indexed_condition(
                attr_name, condition, query_builder, column_prefix
            )
        else:
            op = CONDITION_TO_OP[condition.type]
            if query_builder is None:
                query_builder = []
            query_builder += (
                'token("', column_prefix, attr_name, '")', op, "token(",
                _encode_predefined_attr_value(condition.arg), ")"
            )
            return query_builder

    @classmethod
    def _append_expected_conditions(cls, expected_condition_map, schema,
                                    query_builder, prefix=" IF "):
        if query_builder is None:
            query_builder = []
        for attr_name, cond_list in expected_condition_map.iteritems():
            for condition in cond_list:
                query_builder.append(prefix)
                cls._append_expected_condition(
                    attr_name, condition, query_builder,
                    attr_name in schema.attribute_type_map
                )
                prefix = " AND "
        return query_builder

    @staticmethod
    def _append_expected_condition(attr, condition, query_builder,
                                   is_predefined):
        if query_builder is None:
            query_builder = []
        if condition.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            if condition.arg:
                query_builder += (
                    SYSTEM_COLUMN_ATTR_EXIST, "={'", attr, "'}"
                )
            else:
                if is_predefined:
                    query_builder += (
                        '"', USER_PREFIX, attr, '"=null'
                    )
                else:
                    query_builder += (
                        SYSTEM_COLUMN_EXTRA_ATTR_DATA, "['", attr, "']=null"
                    )
        elif condition.type == models.ExpectedCondition.CONDITION_TYPE_EQUAL:
            if is_predefined:
                query_builder += (
                    '"', USER_PREFIX, attr, '"=',
                    _encode_predefined_attr_value(condition.arg)
                )
            else:
                query_builder += (
                    SYSTEM_COLUMN_EXTRA_ATTR_DATA, "['", attr, "']=",
                    _encode_dynamic_attr_value(condition.arg)
                )
        else:
            assert False
        return query_builder

    @staticmethod
    def _append_primary_key(table_schema, attribute_map, query_builder,
                            prefix=" WHERE "):
        if query_builder is None:
            query_builder = []
        for key_attr in table_schema.key_attributes:
            query_builder += (
                prefix, '"', USER_PREFIX, key_attr, '"=',
                _encode_predefined_attr_value(attribute_map[key_attr])
            )
            prefix = " AND "
        return query_builder

    @staticmethod
    def _append_index_extra_primary_key(query_builder=None,
                                        index_name=None, index_value=None,
                                        prefix=" AND "):
        if query_builder is None:
            query_builder = []

        res_index_values = [
            ENCODED_DEFAULT_STRING_VALUE,
            ENCODED_DEFAULT_STRING_VALUE,
            ENCODED_DEFAULT_NUMBER_VALUE,
            ENCODED_DEFAULT_BLOB_VALUE
        ]

        if index_name:
            res_index_values[0] = _encode_single_value_as_predefined_attr(
                index_name, models.AttributeType.ELEMENT_TYPE_STRING
            )

            res_index_values[INDEX_TYPE_TO_INDEX_POS_MAP[index_value.type]] = (
                _encode_predefined_attr_value(index_value)
            )

        for i in xrange(len(LOCAL_INDEX_FIELD_LIST)):
            query_builder += (
                prefix, LOCAL_INDEX_FIELD_LIST[i], "=", res_index_values[i]
            )
            prefix = " AND "

        return query_builder

    @staticmethod
    def _get_batch_begin_clause(durable=True):
        if durable:
            return 'BEGIN BATCH '
        return 'BEGIN UNLOGGED BATCH '

    @staticmethod
    def _get_batch_apply_clause():
        return ' APPLY BATCH'

    def execute_write_batch(self, context, write_request_list):
        """
        @param context: current request context
        @param write_request_list: contains WriteItemBatchableRequest items to
                    perform batch

        @return: List of unprocessed items

        @raise BackendInteractionException
        """

        assert write_request_list
        unprocessed_items = []
        for req in write_request_list:
            try:
                if isinstance(req, models.PutItemRequest):
                    self.put_item(context, req)
                elif isinstance(req, models.DeleteItemRequest):
                    self.delete_item(context, req)
                else:
                    assert False, 'Wrong WriteItemRequest'
            except BackendInteractionException:
                unprocessed_items.append(req)
                LOG.exception("Can't process WriteItemRequest")

        return unprocessed_items

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

        table_info = self._get_table_info(context, table_name)

        if table_info.schema.index_def_map:
            index_actions = {}
            for index_name, index_def in (
                    table_info.schema.index_def_map.iteritems()):
                attr_name = index_def.attribute_to_index
                action = attribute_action_map.get(
                    attr_name, None
                )
                if action:
                    index_actions[attr_name] = action

            while True:
                old_indexes = self._select_current_index_values(
                    table_info, key_attribute_map
                )

                if old_indexes is None:
                    if expected_condition_map:
                        return False

                    attribute_map = key_attribute_map.copy()
                    for attr_name, attr_action in (
                            attribute_action_map.iteritems()):
                        if attr_action.action in (
                                models.UpdateItemAction.UPDATE_ACTION_PUT,
                                models.UpdateItemAction.UPDATE_ACTION_ADD):
                            attribute_map[attr_name] = attr_action.value
                    if self._put_item_if_not_exists(table_info,
                                                    attribute_map):
                        return True
                    else:
                        continue

                attribute_map = key_attribute_map.copy()
                for attr_name, attr_action in (
                        attribute_action_map.iteritems()):
                    if attr_action.action in (
                            models.UpdateItemAction.UPDATE_ACTION_PUT,
                            models.UpdateItemAction.UPDATE_ACTION_ADD):
                        attribute_map[attr_name] = attr_action.value
                    else:
                        attribute_map[attr_name] = None

                query_builder = self._append_update_query(
                    table_info, attribute_map,
                    expected_condition_map=expected_condition_map
                )

                if_prefix = " AND " if expected_condition_map else " IF "
                for index_name, index_def in (
                        table_info.schema.index_def_map.iteritems()):
                    old_index_value = old_indexes.get(
                        index_def.attribute_to_index, None
                    )
                    query_builder += (
                        if_prefix, '"', USER_PREFIX,
                        index_def.attribute_to_index, '"=',
                        _encode_predefined_attr_value(old_index_value)
                        if old_index_value else "null"
                    )
                    if_prefix = " AND "

                qb_len = len(query_builder)

                self._append_update_indexes_queries(
                    table_info, old_indexes, attribute_map,
                    query_builder
                )
                if len(query_builder) > qb_len:
                    query_builder.insert(0, self._get_batch_begin_clause())
                    query_builder.append(self._get_batch_apply_clause())
                result = self._execute_query("".join(query_builder),
                                             consistent=True)

                if result[0]['[applied]']:
                    return True

                for attr_name, attr_value in old_indexes.iteritems():
                    cas_name = USER_PREFIX + attr_name
                    (_, current_value) = _decode_predefined_attr(
                        table_info, cas_name, result[0][cas_name])
                    if current_value != attr_value:
                        # index consistency condition wasn't passed
                        break
                else:
                    # expected condition wasn't passed
                    return False
        else:
            attribute_map = key_attribute_map.copy()
            for attr_name, attr_action in (
                    attribute_action_map.iteritems()):
                if attr_action.action in (
                        models.UpdateItemAction.UPDATE_ACTION_PUT,
                        models.UpdateItemAction.UPDATE_ACTION_ADD):
                    attribute_map[attr_name] = attr_action.value
                else:
                    attribute_map[attr_name] = None

            query_builder = self._append_update_query(
                table_info, attribute_map,
                expected_condition_map=expected_condition_map
            )

            result = self._execute_query("".join(query_builder),
                                         consistent=True)

            return (result is None) or result[0]['[applied]']

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

        table_info = self._get_table_info(context, table_name)

        assert (
            not index_name or (
                table_info.schema.index_def_map and
                index_name in table_info.schema.index_def_map
            )
        ), "index_name '{}' isn't specified in the schema".format(
            index_name
        )
        select_type = select_type or models.SelectType.all()

        query_builder = [
            "SELECT ", 'COUNT(*)' if select_type.is_count else '*', ' FROM "',
            USER_PREFIX, context.tenant, '"."', table_info.internal_name, '"'
        ]

        hash_name = table_info.schema.key_attributes[0]

        range_name = (
            table_info.schema.key_attributes[1]
            if len(table_info.schema.key_attributes) > 1
            else None
        )

        indexed_attr_name = table_info.schema.index_def_map[
            index_name
        ].attribute_to_index if index_name else None

        hash_key_cond_list = []
        index_attr_cond_list = []
        range_condition_list = []

        if indexed_condition_map:
            indexed_condition_map_copy = indexed_condition_map.copy()
            # Extracting conditions
            if hash_name in indexed_condition_map_copy:
                hash_key_cond_list = indexed_condition_map_copy.pop(hash_name)

            if index_name and (
                    indexed_attr_name in indexed_condition_map_copy):
                index_attr_cond_list = indexed_condition_map_copy.pop(
                    indexed_attr_name
                )
            if range_name and range_name in indexed_condition_map_copy:
                range_condition_list = indexed_condition_map_copy.pop(
                    range_name
                )
            assert not indexed_condition_map_copy

        #processing exclusive_start_key and append conditions
        if exclusive_start_key:
            exclusive_start_key_copy = exclusive_start_key.copy()
            exclusive_hash_key_value = exclusive_start_key_copy.pop(
                hash_name, None
            )
            if exclusive_hash_key_value:
                hash_key_cond_list.append(
                    models.IndexedCondition.eq(exclusive_hash_key_value)
                    if range_name else
                    models.IndexedCondition.gt(exclusive_hash_key_value)
                )

            if index_name:
                exclusive_indexed_value = exclusive_start_key_copy.pop(
                    indexed_attr_name
                )
                index_attr_cond_list.append(
                    models.IndexedCondition.le(exclusive_indexed_value)
                    if order_type == models.ORDER_TYPE_DESC else
                    models.IndexedCondition.ge(exclusive_indexed_value)
                )

            if range_name:
                exclusive_range_value = exclusive_start_key_copy.pop(
                    range_name
                )

                range_condition_list.append(
                    models.IndexedCondition.lt(exclusive_range_value)
                    if order_type == models.ORDER_TYPE_DESC else
                    models.IndexedCondition.gt(exclusive_range_value)
                )
            assert not exclusive_start_key_copy

        prefix = " WHERE "

        if hash_key_cond_list:
            hash_key_cond_list = self._compact_indexed_condition(
                hash_key_cond_list
            )
            if not hash_key_cond_list:
                return models.SelectResult()
        if range_condition_list:
            range_condition_list = self._compact_indexed_condition(
                range_condition_list
            )
            if not range_condition_list:
                return models.SelectResult()
        if index_attr_cond_list:
            index_attr_cond_list = self._compact_indexed_condition(
                index_attr_cond_list
            )
            if not index_attr_cond_list:
                return models.SelectResult()

        if hash_key_cond_list:
            for cond in hash_key_cond_list:
                query_builder.append(prefix)
                self._append_hash_key_indexed_condition(
                    hash_name, cond, query_builder
                )
                prefix = " AND "

        if table_info.schema.index_def_map:
            # append local secondary index related attrs
            local_indexes_conditions = {
                SYSTEM_COLUMN_INDEX_NAME: [
                    models.IndexedCondition.eq(
                        models.AttributeValue.str(index_name)
                        if index_name else DEFAULT_STRING_VALUE
                    )
                ],
                SYSTEM_COLUMN_INDEX_VALUE_STRING: [],
                SYSTEM_COLUMN_INDEX_VALUE_NUMBER: [],
                SYSTEM_COLUMN_INDEX_VALUE_BLOB: []
            }

            default_index_values = [
                DEFAULT_STRING_VALUE,
                DEFAULT_NUMBER_VALUE,
                DEFAULT_BLOB_VALUE
            ]
            if index_attr_cond_list:
                indexed_attr_type = table_info.schema.attribute_type_map[
                    indexed_attr_name
                ]
                n = INDEX_TYPE_TO_INDEX_POS_MAP[indexed_attr_type]
                for i in xrange(1, n):
                    local_indexes_conditions[
                        LOCAL_INDEX_FIELD_LIST[i]
                    ].append(
                        models.IndexedCondition.eq(
                            default_index_values[i-1]
                        )
                    )
                for index_attr_cond in index_attr_cond_list:
                    local_indexes_conditions[
                        LOCAL_INDEX_FIELD_LIST[n]
                    ].append(index_attr_cond)

                if range_condition_list:
                    for i in xrange(n+1, len(LOCAL_INDEX_FIELD_LIST)):
                        local_indexes_conditions[
                            LOCAL_INDEX_FIELD_LIST[i]
                        ].append(
                            models.IndexedCondition.lt(
                                default_index_values[i-1]
                            )
                            if order_type == models.ORDER_TYPE_DESC else
                            models.IndexedCondition.gt(
                                default_index_values[i-1]
                            )
                        )
            elif range_condition_list:
                for i in xrange(1, len(LOCAL_INDEX_FIELD_LIST)):
                        local_indexes_conditions[
                            LOCAL_INDEX_FIELD_LIST[i]
                        ].append(
                            models.IndexedCondition.eq(
                                default_index_values[i-1]
                            )
                        )

            if local_indexes_conditions:
                for cas_field_name, cond_list in (
                        local_indexes_conditions.iteritems()):
                    for cond in cond_list:
                        query_builder.append(prefix)
                        self._append_indexed_condition(
                            cas_field_name, cond, query_builder,
                            column_prefix=""
                        )
                        prefix = " AND "

        if range_condition_list:
            for cond in range_condition_list:
                query_builder.append(prefix)
                self._append_indexed_condition(
                    range_name, cond, query_builder
                )
                prefix = " AND "

        #add limit
        if limit:
            query_builder += (" LIMIT ", str(limit))

        #add ordering
        if order_type:
            query_builder.append(' ORDER BY ')
            if table_info.schema.index_def_map:
                query_builder += (
                    SYSTEM_COLUMN_INDEX_NAME, " ", order_type
                )
            elif range_name:
                query_builder += (
                    '"', USER_PREFIX, range_name, '" ', order_type
                )
            else:
                assert False

        if not hash_key_cond_list or (
                hash_key_cond_list[0].type !=
                models.IndexedCondition.CONDITION_TYPE_EQUAL):
            query_builder.append(" ALLOW FILTERING")

        rows = self._execute_query("".join(query_builder), consistent)

        if select_type.is_count:
            return models.SelectResult(count=rows[0]['count'])

        # process results

        result = []

        # TODO ikhudoshyn: if select_type.is_all_projected,
        # get list of projected attrs by index_name from metainfo

        attributes_to_get = select_type.attributes

        for row in rows:
            record = {}

            #add predefined attributes
            for cas_name, cas_val in row.iteritems():
                if cas_name.startswith(USER_PREFIX) and cas_val:
                    name, val = _decode_predefined_attr(table_info, cas_name,
                                                        cas_val)
                    if not attributes_to_get or name in attributes_to_get:
                        record[name] = val

            #add dynamic attributes (from SYSTEM_COLUMN_ATTR_DATA dict)
            types = row[SYSTEM_COLUMN_EXTRA_ATTR_TYPES]
            attrs = row[SYSTEM_COLUMN_EXTRA_ATTR_DATA] or {}
            for name, val in attrs.iteritems():
                if not attributes_to_get or name in attributes_to_get:
                    typ = types[name]
                    storage_type = CASSANDRA_TO_STORAGE_TYPES[typ]
                    record[name] = _decode_dynamic_value(val, storage_type)

            result.append(record)

        count = len(result)
        if limit and count == limit:
            last_evaluated_key = {hash_name: result[-1][hash_name]}

            if range_name:
                last_evaluated_key[range_name] = result[-1][range_name]

            if index_name:
                indexed_attr_name = table_info.schema.index_def_map[
                    index_name
                ].attribute_to_index
                last_evaluated_key[indexed_attr_name] = result[-1][
                    indexed_attr_name
                ]
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
        if not condition_map:
            condition_map = {}
        table_info = self._get_table_info(context, table_name)
        hash_name = table_info.schema.key_attributes[0]
        try:
            range_name = table_info.schema.key_attributes[1]
        except IndexError:
            range_name = None

        key_conditions = {
            hash_name: []
        }

        if range_name:
            key_conditions[range_name] = []

        if hash_name in condition_map:
            key_conditions[hash_name] = condition_map[hash_name]

            if (range_name and range_name in condition_map
                and condition_map[range_name].type in
                    models.IndexedCondition._allowed_types):

                key_conditions[range_name] = condition_map[range_name]
        if exclusive_start_key:
            if range_name:
                key_conditions[hash_name].append(
                    models.IndexedCondition.eq(exclusive_start_key[hash_name])
                )
                key_conditions[range_name].append(
                    models.IndexedCondition.gt(exclusive_start_key[range_name])
                )
            else:
                key_conditions[hash_name].append(
                    models.IndexedCondition.gt(exclusive_start_key[hash_name])
                )

        selected = self.select_item(context, table_name, key_conditions,
                                    models.SelectType.all(), limit=limit,
                                    consistent=consistent)

        if (range_name and exclusive_start_key
                and range_name in exclusive_start_key
                and (not limit or limit > selected.count)):

            del key_conditions[range_name][-1]
            del key_conditions[hash_name][-1]
            key_conditions[hash_name].append(
                models.IndexedCondition.gt(exclusive_start_key[hash_name])
            )

            limit2 = limit - selected.count if limit else None

            selected2 = self.select_item(
                context, table_name, key_conditions,
                models.SelectType.all(), limit=limit2,
                consistent=consistent)

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


class TableInfo(object):
    SYSTEM_TABLE_TABLE_INFO = SYSTEM_KEYSPACE + '.table_info'

    __field_list = ("schema", "internal_name", "status")

    def __init__(self, storage_driver, tenant, name, schema=None,
                 status=None, internal_name=None):
        self.__storage_driver = storage_driver
        self.__tenant = tenant
        self.__name = name

        self.schema = schema
        self.internal_name = internal_name
        self.status = status

    @property
    def tenant(self):
        return self.__tenant

    @property
    def name(self):
        return self.__name

    @classmethod
    def load(cls, storage_driver, tenant, table_name):
        table_info = TableInfo(storage_driver, tenant, table_name)
        return table_info if table_info.refresh() else None

    @classmethod
    def load_tenant_table_names(cls, storage_driver, tenant,
                                exclusive_start_table_name=None, limit=None):
        query_builder = [
            "SELECT name",
            " FROM ", cls.SYSTEM_TABLE_TABLE_INFO,
            " WHERE tenant='", tenant, "'"
        ]

        if exclusive_start_table_name:
            query_builder += (
                " AND name > '",
                exclusive_start_table_name, "'"
            )

        if limit:
            query_builder += (" LIMIT ", str(limit))

        tables = storage_driver._execute_query("".join(query_builder),
                                               consistent=True)

        return [row['name'] for row in tables]

    def refresh(self, *field_list):
        if not field_list:
            field_list = self.__field_list

        query_builder = ["SELECT "]
        for field in field_list:
            query_builder += ('"', field, '"', ",")
        query_builder.pop()

        query_builder += (
            " FROM ", self.SYSTEM_TABLE_TABLE_INFO,
            " WHERE tenant='", self.tenant, "' AND name='", self.name, "'"
        )

        result = self.__storage_driver._execute_query(
            "".join(query_builder), consistent=True
        )

        if result:
            for name, value in result[0].iteritems():
                if name == "schema":
                    value = models.ModelBase.from_json(value)
                setattr(self, name, value)
            return True
        else:
            return False

    def update(self, *field_list):
        if not field_list:
            field_list = self.__field_list

        query_builder = [
            "UPDATE ", self.SYSTEM_TABLE_TABLE_INFO, " SET "
        ]

        for field in field_list:
            query_builder += (
                '"', field, '"=\'', getattr(self, field), "'", ", "
            )
        query_builder.pop()

        query_builder += (
            " WHERE tenant='", self.tenant, "' AND name='", self.name,
            "' IF exists=1"
        )

        result = self.__storage_driver._execute_query(
            "".join(query_builder), consistent=True
        )

        return result[0]['[applied]']

    def save(self):
        query_builder = [
            "INSERT INTO ", self.SYSTEM_TABLE_TABLE_INFO,
            '(exists, tenant, name, "schema", status, internal_name)'
            "VALUES(1,'", self.tenant, "','", self.name, "'"
        ]

        if self.schema:
            query_builder += (",'", self.schema.to_json(), "'")
        else:
            query_builder.append(",null")

        if self.status:
            query_builder += (",'", self.status, "'")
        else:
            query_builder.append(",null")

        if self.internal_name:
            query_builder += (",'", self.internal_name, "'")
        else:
            query_builder.append(",null")

        query_builder.append(") IF NOT EXISTS")

        result = self.__storage_driver._execute_query(
            "".join(query_builder), consistent=True
        )

        return result[0]['[applied]']

    def delete(self):
        query_builder = [
            "DELETE FROM ", self.SYSTEM_TABLE_TABLE_INFO,
            " WHERE tenant='", self.tenant, "' AND name='", self.name, "'"
        ]

        self.__storage_driver._execute_query("".join(query_builder),
                                             consistent=True)
