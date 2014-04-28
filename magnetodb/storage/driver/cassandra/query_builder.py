import collections
import binascii
import json

from magnetodb.storage import models
from magnetodb.storage.models import AttributeValue
from magnetodb.storage.driver.cassandra import USER_PREFIX
from magnetodb.storage.driver.cassandra import SYSTEM_COLUMN_INDEX_NAME
from magnetodb.storage.driver.cassandra import (
    SYSTEM_COLUMN_INDEX_VALUE_STRING,
    SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
    SYSTEM_COLUMN_INDEX_VALUE_BLOB,
    SYSTEM_COLUMN_ATTR_EXIST,
    SYSTEM_COLUMN_EXTRA_ATTR_DATA,
    SYSTEM_COLUMN_EXTRA_ATTR_TYPES
)


STORAGE_TO_CASSANDRA_TYPES = {
    models.ATTRIBUTE_TYPE_STRING: 'text',
    models.ATTRIBUTE_TYPE_NUMBER: 'decimal',
    models.ATTRIBUTE_TYPE_BLOB: 'blob',
    models.ATTRIBUTE_TYPE_STRING_SET: 'set<text>',
    models.ATTRIBUTE_TYPE_NUMBER_SET: 'set<decimal>',
    models.ATTRIBUTE_TYPE_BLOB_SET: 'set<blob>'
}

CONDITION_TO_OP = {
    models.Condition.CONDITION_TYPE_EQUAL: '=',
    models.IndexedCondition.CONDITION_TYPE_LESS: '<',
    models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL: '<=',
    models.IndexedCondition.CONDITION_TYPE_GREATER: '>',
    models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL: '>=',
}

LOCAL_INDEX_FIELD_LIST = [
    SYSTEM_COLUMN_INDEX_NAME,
    SYSTEM_COLUMN_INDEX_VALUE_STRING,
    SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
    SYSTEM_COLUMN_INDEX_VALUE_BLOB
]

DEFAULT_INDEX_VALUE_LIST = [
    models.AttributeValue.str(''),
    models.AttributeValue.str(''),
    models.AttributeValue.number('0'),
    models.AttributeValue.blob('')
]

INDEX_TYPE_TO_FIELD_POS = {
    models.ATTRIBUTE_TYPE_STRING: 1,
    models.ATTRIBUTE_TYPE_NUMBER: 2,
    models.ATTRIBUTE_TYPE_BLOB: 3
}


def _encode_and_append_predefined_attr_value(attr_value, query_builder):
    if attr_value is None:
        query_builder.append('null')
        return

    attr_type = attr_value.type
    element_type = attr_type.element_type

    if attr_type.collection_type:
        query_builder.append("{")
        if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
            query_builder.append(
                ",".join(map("'{}'".format, attr_value.encoded_value))
            )
        elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            query_builder.append(",".join(attr_value.encoded_value))
        elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            query_builder.append(
                ",".join(
                    [
                        "0x{}".format(
                            binascii.hexlify(binascii.a2b_base64(value))
                        )
                        for value in attr_value.encoded_value
                    ]
                )
            )
        else:
            assert False
        query_builder.append("}")
    else:
        if element_type == models.AttributeType.ELEMENT_TYPE_STRING:
            query_builder.append("'{}'".format(attr_value.encoded_value))
        elif element_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            query_builder.append(attr_value.encoded_value)
        elif element_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            query_builder.append(
                "0x{}".format(binascii.hexlify(attr_value.value))
            )
        else:
            assert False


def _encode_dynamic_attr_value(attr_value):
    if attr_value is None:
        return 'null'
    attr_value.normalize()
    return "0x{}".format(
        binascii.hexlify(json.dumps(attr_value.encoded_value))
    )


def _create_raw_extra_attr_types_system_attr_value(dynamic_attributes):
    return ",".join(
        [
            "'{}':'{}'".format(attr, val.type.value)
            for attr, val in dynamic_attributes
        ]
    )


def _create_raw_exists_system_attr_value(attrs):
    return ",".join(map("'{}'".format, attrs))


def _append_primary_key(table_schema, attribute_map, query_builder,
                        prefix=" WHERE "):
    for key_attr in table_schema.key_attributes:
        query_builder.append(
            '{}"{}{}"='.format(prefix, USER_PREFIX, key_attr)
        )
        _encode_and_append_predefined_attr_value(
            attribute_map[key_attr], query_builder
        )
        prefix = " AND "


def _append_index_extra_primary_key(query_builder, index_name=None,
                                    index_value=None, prefix=" AND "):
        res_index_values = [
            SYSTEM_COLUMN_INDEX_NAME, "",
            SYSTEM_COLUMN_INDEX_VALUE_STRING, "",
            SYSTEM_COLUMN_INDEX_VALUE_NUMBER, 0,
            SYSTEM_COLUMN_INDEX_VALUE_BLOB, ""
        ]

        if index_name:
            res_index_values[1] = index_name

            if index_value.type == models.ATTRIBUTE_TYPE_STRING:
                res_index_values[3] = index_value.encoded_value
            elif index_value.type == models.ATTRIBUTE_TYPE_NUMBER:
                res_index_values[5] = index_value.encoded_value
            elif index_value.type == models.ATTRIBUTE_TYPE_BLOB:
                res_index_values[6] = binascii.hexlify(index_value.value)

        query_builder.append(
            "{}{}='{}' AND {}='{}' AND {}={} AND {}=0x{}".format(
                prefix, *res_index_values
            )
        )


def _append_insert_query(table_info, attribute_map, query_builder,
                         index_name=None, index_value=None,
                         if_not_exist=False):

    query_builder.append(
        'INSERT INTO "{}"."{}" ('.format(
            table_info.internal_keyspace, table_info.internal_name
        )
    )

    schema = table_info.schema
    index_def_map = schema.index_def_map

    if index_def_map:
        query_builder.append(
            "{},{},{},{},".format(*LOCAL_INDEX_FIELD_LIST)
        )

    attr_type_map = schema.attribute_type_map

    predefined_attrs = [
        (n, v) for n, v in attribute_map.iteritems() if n in attr_type_map
    ]

    dynamic_attrs = [
        (n, v) for n, v in attribute_map.iteritems() if n not in attr_type_map
    ]

    query_builder.append(
        "".join(
            [
                '"{}{}",'.format(USER_PREFIX, name)
                for name, _ in predefined_attrs
            ]
        )
    )

    query_builder.append(
        ",".join(
            [
                SYSTEM_COLUMN_EXTRA_ATTR_DATA,
                SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
                SYSTEM_COLUMN_ATTR_EXIST
            ]
        )
    )

    query_builder.append(") VALUES(")

    if index_def_map:
        res_index_values = ["", "", "0", ""]

        if index_name:
            res_index_values[0] = index_name
            index_type = index_value.type

            if index_type == models.ATTRIBUTE_TYPE_STRING:
                res_index_values[1] = index_value.encoded_value
            elif index_type == models.ATTRIBUTE_TYPE_NUMBER:
                res_index_values[3] = index_value.encoded_value
            elif index_type == models.ATTRIBUTE_TYPE_BLOB:
                res_index_values[5] = binascii.hexlify(index_value.value)
            else:
                assert False

        query_builder.append("'{}','{}',{},0x{},".format(*res_index_values))

    for _, attr_value in predefined_attrs:
        _encode_and_append_predefined_attr_value(attr_value, query_builder)
        query_builder.append(",")

    query_builder.append("{")

    prefix = ""
    for attr_name, attr_value in dynamic_attrs:
        query_builder.append("{}'{}':".format(prefix, attr_name))
        query_builder.append(_encode_dynamic_attr_value(attr_value))
        prefix = ","

    query_builder.append(
        "}},{{{}}},{{{}}})".format(
            _create_raw_extra_attr_types_system_attr_value(dynamic_attrs),
            _create_raw_exists_system_attr_value(attribute_map.keys())
        )
    )

    if if_not_exist:
        query_builder.append(" IF NOT EXISTS")


def _append_update_query_with_basic_pk(table_info, attribute_map,
                                       query_builder, rewrite=False):
    schema = table_info.schema
    key_attr_names = schema.key_attributes
    attr_type_map = schema.attribute_type_map

    query_builder.append(
        'UPDATE "{}"."{}" SET '.format(
            table_info.internal_keyspace, table_info.internal_name
        )
    )

    predefined_attrs = [
        (n, v) for n, v in attribute_map.iteritems()
        if n in attr_type_map and n not in key_attr_names
    ]

    dynamic_attrs = [
        (n, v) for n, v in attribute_map.iteritems() if n not in attr_type_map
    ]

    dynamic_attrs_to_set = [
        (n, v) for n, v in dynamic_attrs
        if v is not None
    ]

    dynamic_attr_names_to_delete = [
        n for n, v in dynamic_attrs
        if v is None
    ]

    dynamic_attr_names_to_set = [
        n for n, _ in dynamic_attrs_to_set
    ]

    not_processed_predefined_attr_names = [
        name for name in attr_type_map.iterkeys() if not name in attribute_map
    ]

    set_prefix = ""

    for name, val in predefined_attrs:
        query_builder.append(
            "{}\"{}{}\"=".format(set_prefix, USER_PREFIX, name)
        )
        _encode_and_append_predefined_attr_value(
            val, query_builder=query_builder
        )
        set_prefix = ","

    if rewrite:
        query_builder.append(
            "{}{}={{".format(set_prefix, SYSTEM_COLUMN_EXTRA_ATTR_DATA)
        )

        field_prefix = ""
        for name, value in dynamic_attrs_to_set:
            query_builder.append("{}'{}':".format(field_prefix, name))
            query_builder.append(_encode_dynamic_attr_value(value))
            field_prefix = ","

        query_builder.append("},")

        for name in not_processed_predefined_attr_names:
            query_builder.append('"{}{}"=null,'.format(USER_PREFIX, name))

        query_builder.append(
            "{}={{{}}},{}={{{}}}".format(
                SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
                _create_raw_extra_attr_types_system_attr_value(
                    dynamic_attrs_to_set
                ),
                SYSTEM_COLUMN_ATTR_EXIST,
                _create_raw_exists_system_attr_value(dynamic_attr_names_to_set)
            )
        )
    else:
        if dynamic_attrs_to_set:
            query_builder.append(
                "{}{}={}+{{".format(
                    set_prefix, SYSTEM_COLUMN_EXTRA_ATTR_DATA,
                    SYSTEM_COLUMN_EXTRA_ATTR_DATA
                )
            )

            field_prefix = ""
            for name, value in dynamic_attrs_to_set:
                query_builder.append("{}'{}':".format(field_prefix, name))
                query_builder.append(_encode_dynamic_attr_value(value))
                field_prefix = ","

            query_builder.append(
                "}},{}={}+{{{}}},{}={}+{{{}}}".format(
                    SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
                    SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
                    _create_raw_extra_attr_types_system_attr_value(
                        dynamic_attrs_to_set
                    ),
                    SYSTEM_COLUMN_ATTR_EXIST,
                    SYSTEM_COLUMN_ATTR_EXIST,
                    _create_raw_exists_system_attr_value(
                        dynamic_attr_names_to_set
                    )
                )
            )
            set_prefix = ","
        if dynamic_attr_names_to_delete:
            query_builder.append(set_prefix)
            query_builder.append(
                "".join(
                    [
                        "{}['{}']=null, {}['{}']=null,".format(
                            SYSTEM_COLUMN_EXTRA_ATTR_DATA,
                            name,
                            SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
                            name
                        ) for name in dynamic_attr_names_to_delete
                    ]
                )
            )

            query_builder.append(
                "{}={}-{{{}}}".format(
                    SYSTEM_COLUMN_ATTR_EXIST, SYSTEM_COLUMN_ATTR_EXIST,
                    _create_raw_exists_system_attr_value(
                        dynamic_attr_names_to_delete
                    )
                )
            )

    _append_primary_key(schema, attribute_map, query_builder)


def _append_expected_conditions(expected_condition_map, schema,
                                query_builder, prefix=" IF "):
    for attr_name, cond_list in expected_condition_map.iteritems():
        for condition in cond_list:
            query_builder.append(prefix)
            _append_expected_condition(
                attr_name, condition, query_builder,
                attr_name in schema.attribute_type_map
            )
            prefix = " AND "


def _append_expected_condition(attr, condition, query_builder, is_predefined):
    if condition.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
        if condition.arg:
            query_builder.append(
                "{}={{'{}'}}".format(SYSTEM_COLUMN_ATTR_EXIST, attr)
            )
        else:
            if is_predefined:
                query_builder.append('"{}{}"=null'.format(USER_PREFIX, attr))
            else:
                query_builder.append(
                    "{}['{}']=null".format(SYSTEM_COLUMN_EXTRA_ATTR_DATA, attr)
                )

    elif condition.type == models.ExpectedCondition.CONDITION_TYPE_EQUAL:
        if is_predefined:
            query_builder.append('"{}{}"='.format(USER_PREFIX, attr))
            _encode_and_append_predefined_attr_value(
                condition.arg, query_builder
            )
        else:
            query_builder.append("{}['{}']=".format(
                SYSTEM_COLUMN_EXTRA_ATTR_DATA, attr)
            )
            query_builder.append(_encode_dynamic_attr_value(condition.arg))
    else:
        assert False


def _append_update_query(table_info, attribute_map, query_builder,
                         index_name=None, index_value=None,
                         expected_condition_map=None, rewrite=False):
    _append_update_query_with_basic_pk(
        table_info, attribute_map, query_builder, rewrite=rewrite
    )

    if table_info.schema.index_def_map:
        _append_index_extra_primary_key(query_builder, index_name, index_value)

    if expected_condition_map:
        _append_expected_conditions(
            expected_condition_map, table_info.schema,
            query_builder
        )


def _append_delete_query_with_basic_pk(table_info, attribute_map,
                                       query_builder):
    query_builder.append(
        'DELETE FROM "{}"."{}"'.format(table_info.internal_keyspace,
                                       table_info.internal_name)
    )

    _append_primary_key(table_info.schema, attribute_map, query_builder)


def _append_update_indexes_queries(table_info, old_attribute_map,
                                   attribute_map, query_builder, separator=" ",
                                   rewrite=False):
    base_update_query = None
    base_delete_query = None

    if old_attribute_map is None:
        old_attribute_map = {}

    def create_base_update_query():
        base_query_builder = collections.deque()
        _append_update_query_with_basic_pk(table_info, attribute_map,
                                           base_query_builder, rewrite=rewrite)
        return "".join(base_query_builder)

    def create_base_delete_query():
        base_query_builder = collections.deque()
        _append_delete_query_with_basic_pk(table_info, attribute_map,
                                           base_query_builder)
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
            query_builder.append(separator)
            query_builder.append(base_update_query)

            _append_index_extra_primary_key(
                query_builder, index_name, new_index_value,
            )
        if old_index_value and old_index_value != new_index_value:
            base_delete_query = (
                base_delete_query or create_base_delete_query()
            )
            query_builder.append(separator)
            query_builder.append(base_delete_query)
            _append_index_extra_primary_key(
                query_builder, index_name, old_index_value,
            )


def _append_indexed_condition(attr_name, condition, query_builder,
                              column_prefix=USER_PREFIX):
    query_builder.append('"{}{}"{}'.format(column_prefix, attr_name,
                                           CONDITION_TO_OP[condition.type]))
    _encode_and_append_predefined_attr_value(
        condition.arg, query_builder
    )


def _append_hash_key_indexed_condition(attr_name, condition, query_builder,
                                       column_prefix=USER_PREFIX):
    if condition.type == models.IndexedCondition.CONDITION_TYPE_EQUAL:
        return _append_indexed_condition(
            attr_name, condition, query_builder, column_prefix
        )
    else:
        op = CONDITION_TO_OP[condition.type]
        query_builder.append(
            'token("{}{}"){}token('.format(column_prefix, attr_name, op)
        )
        _encode_and_append_predefined_attr_value(
            condition.arg, query_builder
        )
        query_builder.append(")")


def generate_select_current_index_values_query(table_info, attribute_map):
    query_builder = collections.deque()
    query_builder.append("SELECT ")

    query_builder.append(
        ",".join(
            [
                '"{}{}"'.format(USER_PREFIX, index_def.attribute_to_index)
                for index_def in
                table_info.schema.index_def_map.itervalues()
            ]
        )
    )

    query_builder.append(
        ' FROM "{}"."{}"'.format(
            table_info.internal_keyspace, table_info.internal_name
        )
    )

    _append_primary_key(table_info.schema, attribute_map, query_builder)
    _append_index_extra_primary_key(query_builder, prefix=" AND ")

    return "".join(query_builder)


def generate_delete_query(table_info, key_attribute_map, old_indexes=None,
                          expected_condition_map=None):
    query_builder = collections.deque()
    _append_delete_query_with_basic_pk(
        table_info, key_attribute_map, query_builder
    )

    if not table_info.schema.index_def_map:
        if expected_condition_map:
            _append_expected_conditions(
                expected_condition_map, table_info.schema,
                query_builder
            )
        return "".join(query_builder)

    basic_delete_query = "".join(query_builder)
    query_builder = collections.deque()
    query_builder.append(basic_delete_query)
    _append_index_extra_primary_key(query_builder)
    if expected_condition_map:
        _append_expected_conditions(
            expected_condition_map, table_info.schema,
            query_builder
        )

    if table_info.schema.index_def_map:
        addition_delete_query_builder = collections.deque()

        if_prefix = " AND " if expected_condition_map else " IF "
        for index_name, index_def in (
                table_info.schema.index_def_map.iteritems()):
            index_value = old_indexes.get(
                index_def.attribute_to_index, None
            )
            query_builder.append(
                '{}"{}{}"='.format(
                    if_prefix, USER_PREFIX, index_def.attribute_to_index
                )
            )
            _encode_and_append_predefined_attr_value(
                index_value, query_builder=query_builder
            )
            if_prefix = " AND "

            if index_value:
                addition_delete_query_builder.append(" ")
                addition_delete_query_builder.append(basic_delete_query)
                _append_index_extra_primary_key(addition_delete_query_builder,
                                                index_name, index_value)

        if addition_delete_query_builder:
            query_builder.appendleft("BEGIN UNLOGGED BATCH ")
            query_builder.extend(addition_delete_query_builder)
            query_builder.append(" APPLY BATCH")

    return "".join(query_builder)


def generate_put_query(table_info, attribute_map, old_indexes=None,
                       if_not_exist=False, expected_condition_map=None):
    query_builder = collections.deque()

    if expected_condition_map or old_indexes is not None:
        _append_update_query(table_info, attribute_map, query_builder,
                             expected_condition_map=expected_condition_map,
                             rewrite=True)
        if old_indexes is not None:
            if_prefix = " AND " if expected_condition_map else " IF "
            for index_name, index_def in (
                    table_info.schema.index_def_map.iteritems()):
                index_value = old_indexes.get(
                    index_def.attribute_to_index, None
                )
                query_builder.append(
                    '{}"{}{}"='.format(
                        if_prefix, USER_PREFIX, index_def.attribute_to_index
                    )
                )
                _encode_and_append_predefined_attr_value(
                    index_value, query_builder=query_builder
                )
                if_prefix = " AND "
    else:
        _append_insert_query(
            table_info, attribute_map, query_builder, if_not_exist=if_not_exist
        )

    if table_info.schema.index_def_map:
        qb_len = len(query_builder)
        _append_update_indexes_queries(
            table_info, old_indexes, attribute_map, query_builder,
            rewrite=True
        )
        if len(query_builder) > qb_len:
            query_builder.appendleft("BEGIN UNLOGGED BATCH ")
            query_builder.append(" APPLY BATCH")

    return "".join(query_builder)


def generate_update_query(table_info, attribute_map, old_indexes=None,
                          expected_condition_map=None):
    query_builder = collections.deque()

    _append_update_query(table_info, attribute_map, query_builder,
                         expected_condition_map=expected_condition_map,
                         rewrite=False)
    if table_info.schema.index_def_map:
        if_prefix = " AND " if expected_condition_map else " IF "
        for index_name, index_def in (
                table_info.schema.index_def_map.iteritems()):
            index_value = old_indexes.get(
                index_def.attribute_to_index, None
            )
            query_builder.append(
                '{}"{}{}"='.format(
                    if_prefix, USER_PREFIX, index_def.attribute_to_index
                )
            )
            _encode_and_append_predefined_attr_value(
                index_value, query_builder=query_builder
            )
            if_prefix = " AND "

        qb_len = len(query_builder)
        _append_update_indexes_queries(
            table_info, old_indexes, attribute_map, query_builder,
            rewrite=False
        )
        if len(query_builder) > qb_len:
            query_builder.appendleft("BEGIN UNLOGGED BATCH ")
            query_builder.append(" APPLY BATCH")

    return "".join(query_builder)


def generate_select_query(table_info, hash_key_cond_list, range_condition_list,
                          index_name, index_condition_list, select_type,
                          limit, order_type=None):
    hash_name = table_info.schema.key_attributes[0]

    range_name = (
        table_info.schema.key_attributes[1]
        if len(table_info.schema.key_attributes) > 1
        else None
    )

    indexed_attr_name = table_info.schema.index_def_map[
        index_name
    ].attribute_to_index if index_name else None

    query_builder = collections.deque()
    query_builder.append(
        'SELECT {} FROM "{}"."{}"'.format(
            'COUNT(*)' if select_type.is_count else '*',
            table_info.internal_keyspace, table_info.internal_name
        )
    )

    prefix = " WHERE "

    if hash_key_cond_list:
        for cond in hash_key_cond_list:
            query_builder.append(prefix)
            _append_hash_key_indexed_condition(
                hash_name, cond, query_builder
            )
            prefix = " AND "

    if table_info.schema.index_def_map:
        # append local secondary index related attrs
        local_indexes_conditions = {}

        if index_condition_list:
            local_indexes_conditions[SYSTEM_COLUMN_INDEX_NAME] = [
                models.IndexedCondition.eq(AttributeValue.str(index_name))
            ]

            indexed_attr_type = table_info.schema.attribute_type_map[
                indexed_attr_name
            ]

            n = INDEX_TYPE_TO_FIELD_POS[indexed_attr_type]

            for i in xrange(1, n):
                local_indexes_conditions[LOCAL_INDEX_FIELD_LIST[i]] = [
                    models.IndexedCondition.eq(
                        DEFAULT_INDEX_VALUE_LIST[i]
                    )
                ]

            local_indexes_conditions[
                LOCAL_INDEX_FIELD_LIST[n]
            ] = index_condition_list

            if range_condition_list:
                for i in xrange(n+1, len(LOCAL_INDEX_FIELD_LIST)):
                    local_indexes_conditions[LOCAL_INDEX_FIELD_LIST[i]] = [
                        models.IndexedCondition.lt(
                            DEFAULT_INDEX_VALUE_LIST[i]
                        ) if order_type == models.ORDER_TYPE_DESC else
                        models.IndexedCondition.gt(
                            DEFAULT_INDEX_VALUE_LIST[i]
                        )
                    ]
        else:
            for i in xrange(0, len(LOCAL_INDEX_FIELD_LIST)):
                local_indexes_conditions[LOCAL_INDEX_FIELD_LIST[i]] = [
                    models.IndexedCondition.eq(DEFAULT_INDEX_VALUE_LIST[i])
                ]

        if local_indexes_conditions:
            for cas_field_name, cond_list in (
                    local_indexes_conditions.iteritems()):
                for cond in cond_list:
                    query_builder.append(prefix)
                    _append_indexed_condition(
                        cas_field_name, cond, query_builder,
                        column_prefix=""
                    )
                    prefix = " AND "

    if range_condition_list:
        for cond in range_condition_list:
            query_builder.append(prefix)
            _append_indexed_condition(
                range_name, cond, query_builder
            )
            prefix = " AND "

    #add limit
    if limit:
        query_builder.append(" LIMIT ")
        query_builder.append(str(limit))

    #add ordering
    if order_type:
        query_builder.append(' ORDER BY ')
        if table_info.schema.index_def_map:
            query_builder.append(SYSTEM_COLUMN_INDEX_NAME)
            query_builder.append(" ")
        elif range_name:
            query_builder.append('"')
            query_builder.append(USER_PREFIX)
            query_builder.append(range_name)
            query_builder.append('" ')
        else:
            assert False
        query_builder.append(order_type)

    if not hash_key_cond_list or (
            hash_key_cond_list[0].type !=
            models.IndexedCondition.CONDITION_TYPE_EQUAL):
        query_builder.append(" ALLOW FILTERING")

    return "".join(query_builder)


def generate_create_table_query(cas_keyspace, cas_table_name, table_schema):
    query_builder = collections.deque()
    query_builder.append(
        'CREATE TABLE "{}"."{}"('.format(cas_keyspace, cas_table_name)
    )

    if table_schema.index_def_map:
        query_builder.append(
            "{} text,{} text,{} decimal,{} blob,".format(
                *LOCAL_INDEX_FIELD_LIST
            )
        )

    for attr_name, attr_type in (
            table_schema.attribute_type_map.iteritems()):
        query_builder.append(
            '"{}{}" {},'.format(
                USER_PREFIX, attr_name,
                STORAGE_TO_CASSANDRA_TYPES[attr_type]
            )
        )

    hash_key_name = table_schema.key_attributes[0]
    range_key_name = (
        table_schema.key_attributes[1]
        if len(table_schema.key_attributes) > 1 else None
    )

    query_builder.append(
        '{} map<text, blob>,{} map<text, text>,{} set<text>,'
        'PRIMARY KEY("{}{}"'.format(
            SYSTEM_COLUMN_EXTRA_ATTR_DATA, SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
            SYSTEM_COLUMN_ATTR_EXIST, USER_PREFIX, hash_key_name
        )
    )

    if table_schema.index_def_map:
        query_builder.append(",{},{},{},{}".format(*LOCAL_INDEX_FIELD_LIST))

    if range_key_name:
        query_builder.append(',"{}{}"'.format(USER_PREFIX, range_key_name))

    query_builder.append("))")

    return "".join(query_builder)
