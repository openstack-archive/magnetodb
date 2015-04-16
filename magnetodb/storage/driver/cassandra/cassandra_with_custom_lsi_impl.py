# Copyright 2013 Mirantis Inc.
# Copyright 2014 Symantec Corporation
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

from oslo_serialization import jsonutils as json

from magnetodb.common import exception
from magnetodb.common import probe
from magnetodb.openstack.common import log as logging
from magnetodb.storage import models
from magnetodb.storage import driver
from magnetodb.storage.driver.cassandra import encoder as cas_enc

from oslo_config import cfg
import pyjolokia

LOG = logging.getLogger(__name__)

CONF = cfg.CONF

jmx_opts = [
    cfg.ListOpt('jolokia_endpoint_list',
                help='List of Jolokia endpoints',
                default=['http://127.0.0.1:8778/jolokia/']),
]

CONF.register_opts(jmx_opts)

CONDITION_TO_OP = {
    models.Condition.CONDITION_TYPE_EQUAL: '=',
    models.IndexedCondition.CONDITION_TYPE_LESS: '<',
    models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL: '<=',
    models.IndexedCondition.CONDITION_TYPE_GREATER: '>',
    models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL: '>=',
}

USER_PREFIX = 'u_'
USER_PREFIX_LENGTH = len(USER_PREFIX)

SYSTEM_TABLE_TABLE_INFO = 'magnetodb.table_info'
SYSTEM_COLUMN_QUERY_PARAMETERS = 'q_params'

SYSTEM_COLUMN_EXTRA_ATTR_DATA = 'dyn_attr_dat'
SYSTEM_COLUMN_EXTRA_ATTR_TYPES = 'dyn_attr_typ'
SYSTEM_COLUMN_ATTR_EXIST = 'attr_exist'

DEFAULT_STRING_VALUE = models.AttributeValue('S', decoded_value='')
DEFAULT_NUMBER_VALUE = models.AttributeValue('N', decoded_value=0)
DEFAULT_BLOB_VALUE = models.AttributeValue('B', decoded_value='')


def _decode_predefined_attr(table_info, cas_name, cas_val, prefix=USER_PREFIX):
    assert cas_name.startswith(prefix) and cas_val

    name = cas_name[len(USER_PREFIX):]
    storage_type = table_info.schema.attribute_type_map[name]
    return name, models.AttributeValue(storage_type, decoded_value=cas_val)


def _decode_dynamic_attr_value(value, storage_type):
    value = json.loads(value)

    return models.AttributeValue(storage_type, encoded_value=value)


def _storage_to_cassandra_primitive_type(primitive_type):
    if primitive_type == models.AttributeType.PRIMITIVE_TYPE_STRING:
        return 'text'
    if primitive_type == models.AttributeType.PRIMITIVE_TYPE_NUMBER:
        return 'decimal'
    if primitive_type == models.AttributeType.PRIMITIVE_TYPE_BLOB:
        return 'blob'


def _storage_to_cassandra_type(storage_type):
    if storage_type.collection_type is None:
        return _storage_to_cassandra_primitive_type(storage_type.type)
    if (storage_type.collection_type ==
            models.AttributeType.COLLECTION_TYPE_SET):
        return 'set<{}>'.format(
            _storage_to_cassandra_primitive_type(storage_type.element_type)
        )
    if (storage_type.collection_type ==
            models.AttributeType.COLLECTION_TYPE_MAP):
        return 'map<{},{}>'.format(
            _storage_to_cassandra_primitive_type(storage_type.key_type),
            _storage_to_cassandra_primitive_type(storage_type.value_type)
        )


class CassandraStorageDriverWithCustomLSI(driver.StorageDriver):
    def __init__(self, cluster_handler, default_keyspace_opts):
        self.__cluster_handler = cluster_handler
        self.__default_keyspace_opts = default_keyspace_opts

    @probe.Probe(__name__)
    def create_table(self, tenant, table_info):
        """
        Create table at the backend side

        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information

        :returns: internal_table_name created

        :raises: BackendInteractionException
        """

        table_schema = table_info.schema

        cas_table_name = USER_PREFIX + table_info.id.hex
        cas_keyspace = USER_PREFIX + tenant
        cas_full_table_name = '"{}"."{}"'.format(cas_keyspace, cas_table_name)

        self._create_keyspace_if_not_exists(cas_keyspace)
        key_count = len(table_schema.key_attributes)

        if key_count < 1 or key_count > 2:
            raise exception.BackendInteractionException(
                "Expected 1 or 2 key attribute(s). Found {}: {}".format(
                    key_count, table_schema.key_attributes))

        hash_key_name = table_schema.key_attributes[0]
        range_key_name = (
            table_schema.key_attributes[1] if key_count > 1 else None
        )

        query_builder = [
            'CREATE TABLE ', cas_full_table_name, ' ('
        ]

        for attr_name, attr_type in (
                table_schema.attribute_type_map.iteritems()):

            query_builder += (
                '"', USER_PREFIX, attr_name, '" ',
                _storage_to_cassandra_type(attr_type), ","
            )

        if table_schema.index_def_map:
            query_builder += (
                ",", SYSTEM_COLUMN_QUERY_PARAMETERS, " text,"
            )

        query_builder += (
            SYSTEM_COLUMN_EXTRA_ATTR_DATA, " map<text, blob>,",
            SYSTEM_COLUMN_EXTRA_ATTR_TYPES, " map<text, text>,",
            SYSTEM_COLUMN_ATTR_EXIST, " map<text, int>,",
            'PRIMARY KEY ("',
            USER_PREFIX, hash_key_name, '"'
        )

        if range_key_name:
            query_builder += (
                ',"', USER_PREFIX, range_key_name, '"'
            )

        query_builder.append("))")

        self.__cluster_handler.execute_query("".join(query_builder))
        LOG.debug("Create Table CQL request executed.")

        cas_indexed_field_list = []

        if table_schema.index_def_map:
            query = (
                "CREATE CUSTOM INDEX ON {}(\"{}\") USING "
                "'com.mirantis.magnetodb.cassandra.db.index"
                ".MagnetoDBLocalSecondaryIndex' "
                "WITH OPTIONS = {{'query_properties_field':'true'}}"
            ).format(cas_full_table_name, SYSTEM_COLUMN_QUERY_PARAMETERS)
            self.__cluster_handler.execute_query(query)
            LOG.debug("Create query_properties index CQL request executed.")

            for index_def in table_schema.index_def_map.itervalues():
                cas_indexed_field = USER_PREFIX + index_def.alt_range_key_attr
                cas_indexed_field_list.append(cas_indexed_field)

                query = (
                    "CREATE CUSTOM INDEX ON {}(\"{}\") USING "
                    "'com.mirantis.magnetodb.cassandra.db.index"
                    ".MagnetoDBLocalSecondaryIndex' "
                    "WITH OPTIONS = {{'query_properties_field':'false'}}"
                ).format(
                    cas_full_table_name, cas_indexed_field
                )
                self.__cluster_handler.execute_query(query)
                LOG.debug(
                    "Create index for field['{}'] CQL request executed."
                    .format(index_def.alt_range_key_attr)
                )

        self.__cluster_handler.check_table_status(
            keyspace_name=cas_keyspace,
            table_name=cas_table_name,
            expected_exists=True,
            indexed_field_list=cas_indexed_field_list
        )

        return cas_full_table_name

    @probe.Probe(__name__)
    def delete_table(self, tenant, table_info):
        """
        Delete table from the backend side

        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information

        :raises: BackendInteractionException
        """

        query = 'DROP TABLE IF EXISTS ' + table_info.internal_name

        self.__cluster_handler.execute_query(query)

        LOG.debug("Delete Table CQL request executed. "
                  "Checking table status...")

        internal_name_splited = table_info.internal_name.split(".")

        self.__cluster_handler.check_table_status(
            keyspace_name=internal_name_splited[0][1:-1],
            table_name=internal_name_splited[1][1:-1],
            expected_exists=False
        )

        LOG.debug("Checking table status... Done")

    def _create_keyspace_if_not_exists(self, cas_keyspace):
        replication_info = self.__default_keyspace_opts["replication"]
        replication = ("{'class': '%(class)s',"
                       "'replication_factor': %(replication_factor)s}" %
                       replication_info)
        query_builder = [
            "CREATE KEYSPACE IF NOT EXISTS ",
            cas_keyspace, " WITH replication = ", replication
        ]
        self.__cluster_handler.execute_query("".join(query_builder))

    @staticmethod
    def _append_types_system_attr_value(table_schema, attribute_map,
                                        query_builder=None, prefix=""):
        if query_builder is None:
            query_builder = collections.deque()
        query_builder.append(prefix)
        prefix = ""
        query_builder.append("{")
        for attr, val in attribute_map.iteritems():
            if (val is not None) and (
                    attr not in table_schema.attribute_type_map):
                query_builder += (
                    prefix, "'", attr, "':'", val.attr_type.type, "'"
                )
                prefix = ","
        query_builder.append("}")
        return query_builder

    @staticmethod
    def _append_exists_system_attr_value(attribute_map, query_builder=None,
                                         prefix=""):
        if query_builder is None:
            query_builder = collections.deque()
        query_builder.append(prefix)
        prefix = ""
        query_builder.append("{")
        for attr, _ in attribute_map.iteritems():
            query_builder += (prefix, "'", attr, "':1")
            prefix = ","
        query_builder.append("}")
        return query_builder

    def _append_insert_query(
            self, table_info, attribute_map, query_builder=None,
            if_not_exists=False):
        if query_builder is None:
            query_builder = collections.deque()

        _encode_predefined_attr_value = cas_enc.encode_predefined_attr_value
        _encode_dynamic_attr_value = cas_enc.encode_dynamic_attr_value

        query_builder += (
            'INSERT INTO ', table_info.internal_name, ' ('
        )

        not_processed_predefined_attr_names = set(
            table_info.schema.attribute_type_map.keys()
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
                not_processed_predefined_attr_names.remove(name)
            else:
                dynamic_attr_names.append(name)
                dynamic_attr_values.append(
                    _encode_dynamic_attr_value(val)
                )
        for name in not_processed_predefined_attr_names:
            query_builder += (
                '"', USER_PREFIX, name, '",'
            )
            attr_values.append(_encode_predefined_attr_value(None))

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
            query_builder = collections.deque()

        _encode_predefined_attr_value = cas_enc.encode_predefined_attr_value
        _encode_dynamic_attr_value = cas_enc.encode_dynamic_attr_value

        key_attr_names = table_info.schema.key_attributes

        not_processed_predefined_attr_names = set(
            table_info.schema.attribute_type_map.keys()
        )

        query_builder += (
            'UPDATE ', table_info.internal_name, ' SET '
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
                        SYSTEM_COLUMN_ATTR_EXIST, "['", name, "']=null"
                    )
        self._append_primary_key(table_info.schema, attribute_map,
                                 query_builder)

        return query_builder

    def _append_update_query(self, table_info, attribute_map,
                             query_builder=None, expected_condition_map=None,
                             rewrite=False):
        query_builder = self._append_update_query_with_basic_pk(
            table_info, attribute_map, query_builder, rewrite=rewrite
        )

        if expected_condition_map:
            self._append_expected_conditions(
                expected_condition_map, table_info.schema,
                query_builder
            )

        return query_builder

    def _put_item_if_not_exists(self, table_info, attribute_map):
        query_builder = self._append_insert_query(
            table_info, attribute_map, if_not_exists=True
        )

        result = self.__cluster_handler.execute_query(
            "".join(query_builder), consistent=True)

        return result[0]['[applied]']

    @probe.Probe(__name__)
    def put_item(self, tenant, table_info, attribute_map, return_values=None,
                 if_not_exist=False, expected_condition_map=None):
        """
        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param attribute_map: attribute name to AttributeValue mapping.
                    It defines row key and additional attributes to put
                    item
        :param return_values: model that defines what values should be returned
        :param if_not_exist: put item only is row is new record (It is possible
                    to use only one of if_not_exist and expected_condition_map
                    parameter)
        :param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be put or
                    not

        :returns: True if operation performed, otherwise False

        :raises: BackendInteractionException
        """

        hash_name = table_info.schema.hash_key_name
        return_old = (
            return_values is not None and return_values.type ==
            models.InsertReturnValuesType.RETURN_VALUES_TYPE_ALL_OLD
        )

        if expected_condition_map and hash_name in expected_condition_map:
            hash_conditions = expected_condition_map[hash_name]

            hash_cond_new = [
                cond for cond in hash_conditions
                if cond.type != models.ScanCondition.CONDITION_TYPE_NULL
            ]

            if len(hash_conditions) != len(hash_cond_new):
                if_not_exist = True
                LOG.debug('if_not_exist parameter set to True')

                if hash_cond_new:
                    expected_condition_map[hash_name] = hash_cond_new
                else:
                    del expected_condition_map[hash_name]

        if if_not_exist:
            if expected_condition_map:
                raise exception.ValidationError(
                    "Both expected_condition_map and "
                    "if_not_exist specified"
                )
            if self._put_item_if_not_exists(table_info, attribute_map):
                return True, None
            raise exception.ConditionalCheckFailedException()
        elif return_old:
            range_name = table_info.schema.range_key_name

            while True:
                old_item = self._get_item_to_update(
                    tenant, table_info, attribute_map
                )
                if not self._conditions_satisfied(
                        old_item, expected_condition_map):
                    raise exception.ConditionalCheckFailedException()
                if old_item is None:
                    if self._put_item_if_not_exists(table_info,
                                                    attribute_map):
                        return True, old_item
                else:
                    key_attributes = [hash_name]
                    if range_name:
                        key_attributes.append(range_name)
                    conditions = self._get_update_conditions(
                        key_attributes, old_item
                    )
                    query_builder = self._append_update_query(
                        table_info, attribute_map,
                        expected_condition_map=conditions, rewrite=True
                    )

                    result = self.__cluster_handler.execute_query(
                        "".join(query_builder), consistent=True
                    )

                    if result[0]['[applied]']:
                        return True, old_item
        elif expected_condition_map:
            query_builder = self._append_update_query(
                table_info, attribute_map,
                expected_condition_map=expected_condition_map, rewrite=True
            )
            result = self.__cluster_handler.execute_query(
                "".join(query_builder), consistent=True
            )
            if result[0]['[applied]']:
                return True, None
            raise exception.ConditionalCheckFailedException()
        else:
            query_builder = self._append_insert_query(
                table_info, attribute_map
            )
            self.__cluster_handler.execute_query("".join(query_builder),
                                                 consistent=True)
            return True, None

    def batch_write(self, tenant, write_request_list):
        query_builder = []

        for table_info, write_request in write_request_list:
            if write_request.is_put:
                self._append_insert_query(
                    table_info, write_request.attribute_map,
                    query_builder=query_builder
                )
            elif write_request.is_delete:
                self._append_delete_query(
                    table_info, write_request.attribute_map,
                    query_builder=query_builder
                )
            query_builder.append(" ")

        if len(write_request_list) > 1:
            query_builder.insert(0, 'BEGIN UNLOGGED BATCH ')
            query_builder.append(' APPLY BATCH')

        self.__cluster_handler.execute_query("".join(query_builder), True)

    @classmethod
    def _append_delete_query_with_basic_pk(
            cls, table_info, attribute_map, query_builder=None):
        if query_builder is None:
            query_builder = collections.deque()
        query_builder += (
            'DELETE FROM ', table_info.internal_name
        )
        cls._append_primary_key(table_info.schema, attribute_map,
                                query_builder)
        return query_builder

    @classmethod
    def _append_delete_query(
            cls, table_info, attribute_map, query_builder=None,
            expected_condition_map=None):
        query_builder = cls._append_delete_query_with_basic_pk(
            table_info, attribute_map, query_builder)

        if expected_condition_map:
            cls._append_expected_conditions(
                expected_condition_map, table_info.schema,
                query_builder
            )

        return query_builder

    @probe.Probe(__name__)
    def delete_item(self, tenant, table_info, key_attribute_map,
                    expected_condition_map=None):
        """
        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param key_attribute_map: key attribute name to
                    AttributeValue mapping. It defines row to be deleted
        :param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be
                    deleted or not

        :returns: True if operation performed, otherwise False (if operation
                    was skipped by out of date timestamp, it is considered as
                    successfully performed)

        :raises: BackendInteractionException
        """

        delete_query = "".join(
            self._append_delete_query(
                table_info, key_attribute_map,
                expected_condition_map=expected_condition_map
            )
        )

        result = self.__cluster_handler.execute_query(delete_query,
                                                      consistent=True)
        if result and not result[0]['[applied]']:
            raise exception.ConditionalCheckFailedException()
        return True

    @staticmethod
    def _compact_indexed_condition(cond_list):
        left_condition = None
        right_condition = None
        exact_condition = None

        assert cond_list

        for condition in cond_list:
            if condition.type == models.IndexedCondition.CONDITION_TYPE_EQUAL:
                if (exact_condition is not None and
                        condition.arg.decoded_value !=
                        exact_condition.arg.decoded_value):
                    return None
                exact_condition = condition
            elif condition.is_left_border():
                if left_condition is None:
                    left_condition = condition
                elif condition.is_strict_border():
                    if (condition.arg.decoded_value >=
                            left_condition.arg.decoded_value):
                        left_condition = condition
                else:
                    if (condition.arg.decoded_value >
                            left_condition.arg.decoded_value):
                        left_condition = condition
            elif condition.is_right_border():
                if right_condition is None:
                    right_condition = condition
                elif condition.is_strict():
                    if (condition.arg.decoded_value <=
                            right_condition.arg.decoded_value):
                        right_condition = condition
                else:
                    if (condition.arg.decoded_value <
                            right_condition.arg.decoded_value):
                        right_condition = condition

        if exact_condition is not None:
            if left_condition is not None:
                if left_condition.is_strict():
                    if (left_condition.arg.decoded_value >=
                            exact_condition.arg.decoded_value):
                        return None
                else:
                    if (left_condition.arg.decoded_value >
                            exact_condition.arg.decoded_value):
                        return None
            if right_condition is not None:
                if right_condition.is_strict():
                    if (right_condition.arg.decoded_value <=
                            exact_condition.arg.decoded_value):
                        return None
                else:
                    if (right_condition.arg.decoded_value <
                            exact_condition.arg.decoded_value):
                        return None
            return [exact_condition]
        elif left_condition is not None:
            if right_condition is not None:
                if (left_condition.is_strict_border() or
                        right_condition.is_strict_border()):
                    if (left_condition.arg.decoded_value >=
                            right_condition.arg.decoded_value):
                        return None
                else:
                    if (left_condition.arg.decoded_value >
                            right_condition.arg.decoded_value):
                        return None
                return [left_condition, right_condition]
            else:
                return [left_condition]

        assert right_condition is not None

        return [right_condition]

    @staticmethod
    def _append_indexed_condition(attr_name, condition, query_builder,
                                  column_prefix=USER_PREFIX):
        if query_builder is None:
            query_builder = collections.deque()
        op = CONDITION_TO_OP[condition.type]
        query_builder += (
            '"', column_prefix, attr_name, '"', op,
            cas_enc.encode_predefined_attr_value(condition.arg)
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
                query_builder = collections.deque()
            query_builder += (
                'token("', column_prefix, attr_name, '")', op, "token(",
                cas_enc.encode_predefined_attr_value(condition.arg), ")"
            )
            return query_builder

    @classmethod
    def _append_expected_conditions(cls, expected_condition_map, schema,
                                    query_builder, prefix=" IF "):
        if query_builder is None:
            query_builder = collections.deque()
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
            query_builder = collections.deque()

        if condition.type == models.ExpectedCondition.CONDITION_TYPE_NOT_NULL:
            query_builder += (
                SYSTEM_COLUMN_ATTR_EXIST, "['", attr, "']=1"
            )
        elif condition.type == models.ExpectedCondition.CONDITION_TYPE_NULL:
            query_builder += (
                SYSTEM_COLUMN_ATTR_EXIST, "['", attr, "']=null"
            )
        elif condition.type == models.ExpectedCondition.CONDITION_TYPE_EQUAL:
            if is_predefined:
                query_builder += (
                    '"', USER_PREFIX, attr, '"=',
                    cas_enc.encode_predefined_attr_value(condition.arg)
                )
            else:
                query_builder += (
                    SYSTEM_COLUMN_EXTRA_ATTR_DATA, "['", attr, "']=",
                    cas_enc.encode_dynamic_attr_value(condition.arg)
                )
        else:
            assert False
        return query_builder

    @staticmethod
    def _append_primary_key(table_schema, attribute_map, query_builder,
                            prefix=" WHERE "):
        if query_builder is None:
            query_builder = collections.deque()

        _encode_predefined_attr_value = cas_enc.encode_predefined_attr_value

        for key_attr in table_schema.key_attributes:
            if key_attr:
                query_builder += (
                    prefix, '"', USER_PREFIX, key_attr, '"=',
                    _encode_predefined_attr_value(attribute_map[key_attr])
                )
                prefix = " AND "
        return query_builder

    @probe.Probe(__name__)
    def update_item(self, tenant, table_info, key_attribute_map,
                    attribute_action_map, expected_condition_map={}):
        """
        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param key_attribute_map: key attribute name to
            AttributeValue mapping. It defines row it to update item
        :param attribute_action_map: attribute name to UpdateItemAction
            instance mapping. It defines actions to perform for each
            given attribute
        :param expected_condition_map: expected attribute name to
            ExpectedCondition instance mapping. It provides
            preconditions to make decision about should item be updated
            or not
        :returns: True if operation performed, otherwise False
        :raises: BackendInteractionException
        """
        attribute_action_map = attribute_action_map or {}

        while True:
            old_item = self._get_item_to_update(tenant, table_info,
                                                key_attribute_map)
            if not self._conditions_satisfied(old_item,
                                              expected_condition_map):
                raise exception.ConditionalCheckFailedException()

            if old_item is None:
                # updating non-existent item
                attribute_map = key_attribute_map.copy()
                delete_action_only = True
                for attr_name, attr_action in (
                        attribute_action_map.iteritems()):
                    if (attr_action.action ==
                            models.UpdateItemAction.UPDATE_ACTION_PUT or
                        attr_action.action ==
                            models.UpdateItemAction.UPDATE_ACTION_ADD):
                        delete_action_only = False
                        attribute_map[attr_name] = attr_action.value
                if delete_action_only:
                    # ignore DELETE action only update_item request for
                    # non-existent item
                    return True, old_item
                if self._put_item_if_not_exists(table_info,
                                                attribute_map):
                    return True, old_item
            else:
                attribute_map = key_attribute_map.copy()
                for attr_name, attr_action in (
                        attribute_action_map.iteritems()):
                    if (attr_action.action ==
                            models.UpdateItemAction.UPDATE_ACTION_PUT):
                        attribute_map[attr_name] = attr_action.value
                    elif (attr_action.action ==
                            models.UpdateItemAction.UPDATE_ACTION_ADD):
                        attribute_map[attr_name] = self._get_add_attr_value(
                            attr_name, attr_action.value, old_item
                        )
                    else:
                        attribute_map[attr_name] = self._get_del_attr_value(
                            attr_name, attr_action.value, old_item
                        )

                update_conditions = self._get_update_conditions(
                    key_attribute_map, old_item
                )

                query_builder = self._append_update_query(
                    table_info, attribute_map,
                    expected_condition_map=update_conditions
                )

                result = self.__cluster_handler.execute_query(
                    "".join(query_builder), consistent=True
                )

                if not result or result[0]['[applied]']:
                    return True, old_item

    def _get_item_to_update(self, tenant, table_info, key_attribute_map):
        hash_key_value = key_attribute_map.get(
            table_info.schema.hash_key_name, None
        )
        range_key_value = key_attribute_map.get(
            table_info.schema.range_key_name, None
        )
        hash_condition_list = [models.IndexedCondition.eq(hash_key_value)]
        range_condition_list = None if range_key_value is None else [
            models.IndexedCondition.eq(range_key_value)
        ]

        items = self.select_item(
            tenant, table_info, hash_condition_list, range_condition_list,
            select_type=models.SelectType.all(), consistent=True
        ).items
        return items[0] if items else None

    @staticmethod
    def _get_del_attr_value(attr_name, attr_value, old_item):
        # We have no value, just remove an attr
        if not attr_value or not old_item:
            return None

        old_attr_value = old_item.get(attr_name, None)
        if old_attr_value is None:
            return None

        if old_attr_value.is_set:
            if old_attr_value.attr_type != attr_value.attr_type:
                raise exception.InvalidQueryParameter(
                    "Wrong type for %s" % attr_name
                )

            return models.AttributeValue(
                old_attr_value.attr_type,
                decoded_value=(
                    old_attr_value.decoded_value - attr_value.decoded_value
                )
            )

        if old_attr_value.is_map:
            if (not attr_value.is_set or
                    attr_value.attr_type.element_type !=
                    old_attr_value.attr_type.key_type):
                raise exception.InvalidQueryParameter(
                    "Wrong type for %s" % attr_name
                )
            res = old_attr_value.decoded_value.copy()
            for key in attr_value.decoded_value:
                res.pop(key, None)
            return models.AttributeValue(
                old_attr_value.attr_type, decoded_value=res
            )

    @staticmethod
    def _get_add_attr_value(attr_name, attr_value, old_item):
        if not old_item:
            return attr_value

        if (attr_value.attr_type.collection_type is None and
                not attr_value.is_number):
            raise exception.InvalidQueryParameter(
                "ADD allows collections or numbers only"
            )

        if attr_name not in old_item:
            return attr_value

        if old_item[attr_name].attr_type != attr_value.attr_type:
            raise exception.InvalidQueryParameter(
                "Wrong type for %s" % attr_name
            )

        if attr_name not in old_item:
            return attr_value

        if attr_value.is_number:
            return models.AttributeValue(
                old_item[attr_name].attr_type,
                decoded_value=(
                    old_item[attr_name].decoded_value +
                    attr_value.decoded_value
                )
            )

        if attr_value.is_set:
            return models.AttributeValue(
                old_item[attr_name].attr_type,
                decoded_value=old_item[attr_name].decoded_value.union(
                    attr_value.decoded_value
                )
            )

        if attr_value.is_map:
            res = old_item[attr_name].decoded_value.copy()
            res.update(attr_value.decoded_value)

            return models.AttributeValue(old_item[attr_name].attr_type,
                                         decoded_value=res)
        assert False

    @staticmethod
    def _get_update_conditions(key_attribute_map, old_item):
        conditions = {}
        if old_item:
            for attr_name, attr_value in old_item.iteritems():
                if attr_name in key_attribute_map:
                    continue
                conditions[attr_name] = [
                    models.ExpectedCondition.eq(attr_value)]
        return conditions

    @probe.Probe(__name__)
    def select_item(self, tenant, table_info, hash_key_condition_list,
                    range_key_to_query_condition_list, select_type,
                    index_name=None, limit=None, exclusive_start_key=None,
                    consistent=True, order_type=None):
        """
        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param hash_key_condition_list: list of IndexedCondition instances.
                    Defines conditions for hash key to perform query on
        :param range_key_to_query_condition_list: list of IndexedCondition
                    instances. Defines conditions for range key or indexed
                    attribute to perform query on
        :param select_type: SelectType instance. It defines with attributes
                    will be returned. If not specified, default will be used:
                    SelectType.all() for query on table and
                    SelectType.all_projected() for query on index
        :param index_name: String, name of index to search with
        :param limit: maximum count of returned values
        :param exclusive_start_key: key attribute names to AttributeValue
                    instance
        :param consistent: define is operation consistent or not (by default it
                    is not consistent)
        :param order_type: defines order of returned rows, if 'None' - default
                    order will be used

        :returns: SelectResult instance

        :raises: BackendInteractionException
        """

        query_builder = collections.deque(
            (
                "SELECT ", 'COUNT(*)' if select_type.is_count else '*',
                ' FROM ', table_info.internal_name
            )
        )

        hash_name = table_info.schema.key_attributes[0]

        range_name = (
            table_info.schema.key_attributes[1]
            if len(table_info.schema.key_attributes) > 1
            else None
        )

        indexed_attr_name = table_info.schema.index_def_map[
            index_name
        ].alt_range_key_attr if index_name else None

        hash_key_cond_list = []
        index_attr_cond_list = []
        range_condition_list = []

        if hash_key_condition_list is not None:
            hash_key_cond_list.extend(hash_key_condition_list)
        if range_key_to_query_condition_list is not None:
            if index_name is not None:
                index_attr_cond_list.extend(range_key_to_query_condition_list)
            else:
                range_condition_list.extend(range_key_to_query_condition_list)

        # processing exclusive_start_key and append conditions
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

        request_needed = True
        if hash_key_cond_list:
            hash_key_cond_list = self._compact_indexed_condition(
                hash_key_cond_list
            )
            if not hash_key_cond_list:
                request_needed = False
        if request_needed and range_condition_list:
            range_condition_list = self._compact_indexed_condition(
                range_condition_list
            )
            if not range_condition_list:
                request_needed = False
        if request_needed and index_attr_cond_list:
            index_attr_cond_list = self._compact_indexed_condition(
                index_attr_cond_list
            )
            if not index_attr_cond_list:
                request_needed = False

        if request_needed:
            prefix = " WHERE "

            if hash_key_cond_list:
                for cond in hash_key_cond_list:
                    query_builder.append(prefix)
                    self._append_hash_key_indexed_condition(
                        hash_name, cond, query_builder
                    )
                    prefix = " AND "

            if index_attr_cond_list:
                assert hash_key_cond_list is not None

                for cond in index_attr_cond_list:
                    query_builder.append(prefix)
                    self._append_indexed_condition(
                        indexed_attr_name, cond, query_builder
                    )

            if range_condition_list:
                assert hash_key_cond_list is not None

                for cond in range_condition_list:
                    query_builder.append(prefix)
                    self._append_indexed_condition(
                        range_name, cond, query_builder
                    )

            # add ordering
            if index_name:
                query_builder.append(
                    " AND {} = 'ORDER:{}'".format(
                        SYSTEM_COLUMN_QUERY_PARAMETERS,
                        models.ORDER_TYPE_DESC
                        if order_type == models.ORDER_TYPE_DESC else
                        models.ORDER_TYPE_ASC
                    )
                )
            elif range_name:
                if order_type is not None:
                    query_builder += (
                        ' ORDER BY "', USER_PREFIX, range_name, '" ',
                        order_type
                    )

            # add limit
            if limit:
                query_builder += (" LIMIT ", str(limit))

            if index_attr_cond_list:
                query_builder.append(" ALLOW FILTERING")

            rows = self.__cluster_handler.execute_query(
                "".join(query_builder), consistent
            )
        else:
            rows = []

        if select_type.is_count:
            count = rows[0]['count'] if rows else 0
            return models.SelectResult(count=count)

        # process results

        result = []

        # TODO(ikhudoshyn): if select_type.is_all_projected,
        # get list of projected attrs by index_name from metainfo

        attributes_to_get = select_type.attributes

        for row in rows:
            record = {}

            # add predefined attributes
            for cas_name, cas_val in row.iteritems():
                if cas_name.startswith(USER_PREFIX) and cas_val:
                    name, val = _decode_predefined_attr(table_info, cas_name,
                                                        cas_val)
                    if not attributes_to_get or name in attributes_to_get:
                        record[name] = val

            # add dynamic attributes (from SYSTEM_COLUMN_ATTR_DATA dict)
            types = row[SYSTEM_COLUMN_EXTRA_ATTR_TYPES]
            attrs = row[SYSTEM_COLUMN_EXTRA_ATTR_DATA] or {}
            for name, val in attrs.iteritems():
                if not attributes_to_get or name in attributes_to_get:
                    typ = types[name]
                    storage_type = models.AttributeType(typ)
                    record[name] = _decode_dynamic_attr_value(
                        val, storage_type
                    )

            result.append(record)

        count = len(result)
        if limit and count == limit:
            last_evaluated_key = {hash_name: result[-1][hash_name]}

            if range_name:
                last_evaluated_key[range_name] = result[-1][range_name]

            if index_name:
                indexed_attr_name = table_info.schema.index_def_map[
                    index_name
                ].alt_range_key_attr
                last_evaluated_key[indexed_attr_name] = result[-1][
                    indexed_attr_name
                ]
        else:
            last_evaluated_key = None

        return models.SelectResult(items=result,
                                   last_evaluated_key=last_evaluated_key,
                                   count=count)

    @probe.Probe(__name__)
    def scan(self, tenant, table_info, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None, consistent=False):
        """
        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param condition_map: indexed attribute name to list of
                    ScanCondition instances mapping. It defines rows
                    set to be selected
        :param limit: maximum count of returned values
        :param exclusive_start_key: key attribute names to AttributeValue
                    instance
        :param consistent: define is operation consistent or not (by default it
                    is not consistent)

        :returns: list of attribute name to AttributeValue mappings

        :raises: BackendInteractionException
        """
        if not condition_map:
            condition_map = {}

        hash_name = table_info.schema.hash_key_name
        range_name = table_info.schema.range_key_name

        hash_key_condition_list = condition_map.get(hash_name, None)
        range_key_condition_list = None
        if hash_key_condition_list:
            hash_key_condition_list = [
                cond for cond in hash_key_condition_list
                if isinstance(cond, models.IndexedCondition)
            ]
            range_key_condition_list = (
                condition_map.get(range_name, None) if range_name else None
            )
        if hash_key_condition_list:
            if range_key_condition_list:
                range_key_condition_list = [
                    cond for cond in range_key_condition_list
                    if isinstance(cond, models.IndexedCondition)
                ]

        selected = self.select_item(
            tenant, table_info, hash_key_condition_list,
            range_key_condition_list, models.SelectType.all(), limit=limit,
            exclusive_start_key=exclusive_start_key, consistent=consistent
        )

        if ((not limit or limit > selected.count) and
                exclusive_start_key is not None):
            if hash_key_condition_list is None:
                hash_key_condition_list = []
            hash_key_condition_list.append(
                models.IndexedCondition.gt(exclusive_start_key[hash_name])
            )

            limit2 = limit - selected.count if limit else None

            selected2 = self.select_item(
                tenant, table_info, hash_key_condition_list,
                range_key_condition_list, models.SelectType.all(),
                limit=limit2, consistent=consistent)

            selected = models.SelectResult(
                items=selected.items + selected2.items,
                last_evaluated_key=selected2.last_evaluated_key,
                count=selected.count + selected2.count
            )

        scanned_count = selected.count

        if selected.items:
            filtered_items = filter(
                lambda attr_item: self._conditions_satisfied(
                    attr_item, condition_map),
                selected.items)
            count = len(filtered_items)
        else:
            filtered_items = []
            count = selected.count

        if attributes_to_get and filtered_items:
            for item in filtered_items:
                for attr in item.keys():
                    if attr not in attributes_to_get:
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
                        row.get(attr_name, None) if row else None, cond):
                    return False
        return True

    @staticmethod
    def _condition_satisfied(attr_val, cond):
        if cond.type == models.ExpectedCondition.CONDITION_TYPE_NULL:
            return attr_val is None

        if cond.type == models.ExpectedCondition.CONDITION_TYPE_NOT_NULL:
            return attr_val is not None

        if attr_val is None:
            return False

        if cond.type == models.Condition.CONDITION_TYPE_EQUAL:
            return (attr_val.attr_type == cond.arg.attr_type and
                    attr_val.decoded_value == cond.arg.decoded_value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_LESS:
            return (attr_val.attr_type == cond.arg.attr_type and
                    attr_val.decoded_value < cond.arg.decoded_value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL:
            return (attr_val.attr_type == cond.arg.attr_type and
                    attr_val.decoded_value <= cond.arg.decoded_value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_GREATER:
            return (attr_val.attr_type == cond.arg.attr_type and
                    attr_val.decoded_value > cond.arg.decoded_value)

        if (cond.type ==
                models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL):
            return (attr_val.attr_type == cond.arg.attr_type and
                    attr_val.decoded_value >= cond.arg.decoded_value)

        if cond.type == models.ScanCondition.CONDITION_TYPE_NOT_EQUAL:
            return (attr_val.attr_type != cond.arg.attr_type or
                    attr_val.decoded_value != cond.arg.decoded_value)

        if cond.type == models.ScanCondition.CONDITION_TYPE_CONTAINS:
            assert not cond.arg.attr_type.collection_type
            collection_type = attr_val.attr_type.collection_type
            arg_type = cond.arg.attr_type.type
            if collection_type is None:
                val_type = attr_val.attr_type.type
                if (val_type != arg_type or val_type ==
                        models.AttributeType.PRIMITIVE_TYPE_NUMBER):
                    return False
                return cond.arg.decoded_value in attr_val.decoded_value
            elif collection_type == models.AttributeType.COLLECTION_TYPE_SET:
                val_type = attr_val.attr_type.element_type
                if val_type != arg_type:
                    return False
                return cond.arg.decoded_value in attr_val.decoded_value
            elif collection_type == models.AttributeType.COLLECTION_TYPE_MAP:
                key_type = attr_val.attr_type.key_type
                if key_type != arg_type:
                    return False
                return cond.arg.decoded_value in attr_val.decoded_value
            return False

        if cond.type == models.ScanCondition.CONDITION_TYPE_NOT_CONTAINS:
            assert not cond.arg.attr_type.collection_type
            collection_type = attr_val.attr_type.collection_type
            arg_type = cond.arg.attr_type.type
            if collection_type is None:
                val_type = attr_val.attr_type.type
                if (val_type != arg_type or val_type ==
                        models.AttributeType.PRIMITIVE_TYPE_NUMBER):
                    return False
                return cond.arg.decoded_value not in attr_val.decoded_value
            elif collection_type == models.AttributeType.COLLECTION_TYPE_SET:
                val_type = attr_val.attr_type.element_type
                if val_type != arg_type:
                    return False
                return cond.arg.decoded_value not in attr_val.decoded_value
            elif collection_type == models.AttributeType.COLLECTION_TYPE_MAP:
                key_type = attr_val.attr_type.key_type
                if key_type != arg_type:
                    return False
                return cond.arg.decoded_value not in attr_val.decoded_value
            return False

        if cond.type == models.ScanCondition.CONDITION_TYPE_IN:
            cond_args = cond.args or []

            return attr_val in cond_args

        return False

    def health_check(self):
        query = "SELECT * FROM {} LIMIT 1".format(SYSTEM_TABLE_TABLE_INFO)
        try:
            self.__cluster_handler.execute_query(query, consistent=True)
        except Exception as ex:
            LOG.debug(ex)
            raise exception.BackendInteractionError(
                "Can't perform healthcheck query. Error: " + ex.message)
        return True

    def get_table_statistics(self, tenant, table_info, keys):

        tn = table_info.internal_name.split(".", 2)
        table_name = tn[1].strip('"')

        vals = {
            'user_prefix': USER_PREFIX,
            'tenant': tenant,
            'table_name': table_name,
        }

        metrics = {
            'item_count': {
                'type': 'exec',
                'kwargs': {
                    'mbean': 'org.apache.cassandra.db:type=ColumnFamilies,'
                             'keyspace={user_prefix}{tenant},'
                             'columnfamily='
                             '{table_name}'.format(**vals),
                    'operation': 'estimateKeys',
                },
            },
            'size': {
                'type': 'read',
                'kwargs': {
                    'mbean': 'org.apache.cassandra.metrics:type=ColumnFamily,'
                             'keyspace={user_prefix}{tenant},'
                             'scope={table_name},'
                             'name=TotalDiskSpaceUsed'.format(**vals),
                },
            },
        }

        result = {}
        for jmx_node in CONF.jolokia_endpoint_list:
            monitoring = pyjolokia.Jolokia(jmx_node)
            for k in keys:
                monitoring.add_request(metrics[k]['type'],
                                       **metrics[k]['kwargs'])
            data = monitoring.getRequests()
            for key, item in zip(keys, data):
                result.setdefault(key, 0)
                if 'TotalDiskSpaceUsed' in item['request']['mbean']:
                    result[key] += item['value']['Count']
                else:
                    result[key] += item['value']
        r_factor = float(self.__default_keyspace_opts['replication']
                                                     ['replication_factor'])
        for k in result:
            result[k] = int(round(result[k] / r_factor))
        return result
