# Copyright 2014 Mirantis Inc.
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
import json
from magnetodb.storage.models import IndexDefinition
from magnetodb.storage.models import UpdateItemAction
from magnetodb.storage.models import TableMeta
from magnetodb.storage.models import Condition
from magnetodb.storage.models import IndexedCondition
from magnetodb.storage.models import ScanCondition
from magnetodb.storage.models import ExpectedCondition
from magnetodb.storage.models import SelectType
from magnetodb.storage.models import AttributeType
from magnetodb.storage.models import AttributeValue
from magnetodb.storage.models import PutItemRequest
from magnetodb.storage.models import DeleteItemRequest
from magnetodb.storage.models import GetItemRequest

from magnetodb.common.exception import ValidationError


class Props():
    TABLE_NAME = "table_name"
    ATTRIBUTE_DEFINITIONS = "attribute_definitions"
    ATTRIBUTE_NAME = "attribute_name"
    ATTRIBUTE_TYPE = "attribute_type"
    KEY_SCHEMA = "key_schema"
    KEY_TYPE = "key_type"
    LOCAL_SECONDARY_INDEXES = "local_secondary_indexes"
    GLOBAL_SECONDARY_INDEXES = "global_secondary_indexes"
    INDEX_NAME = "index_name"
    PROJECTION = "projection"
    NON_KEY_ATTRIBUTES = "non_key_attributes"
    PROJECTION_TYPE = "projection_type"

    TABLE_DESCRIPTION = "table_description"
    TABLE = "table"
    TABLE_SIZE_BYTES = "table_size_bytes"
    TABLE_STATUS = "table_status"
    CREATION_DATE_TIME = "creation_date_time"
    INDEX_SIZE_BYTES = "index_size_bytes"
    ITEM_COUNT = "item_count"

    TABLE_NAMES = "table_names"
    EXCLUSIVE_START_TABLE_NAME = "exclusive_start_table_name"
    LAST_EVALUATED_TABLE_NAME = "last_evaluated_table_name"
    LIMIT = "limit"

    EXPECTED = "expected"
    EXISTS = "exists"
    VALUE = "value"
    ITEM = "item"
    RETURN_VALUES = "return_values"

    ATTRIBUTES = "attributes"
    ITEM_COLLECTION_METRICS = "item_collection_metrics"
    ITEM_COLLECTION_KEY = "item_collection_key"

    ATTRIBUTES_TO_GET = "attributes_to_get"
    CONSISTENT_READ = "consistent_read"
    KEY = "key"
    KEYS = "keys"

    EXCLUSIVE_START_KEY = "exclusive_start_key"
    SCAN_FILTER = "scan_filter"
    SELECT = "select"
    SEGMENT = "segment"
    TOTAL_SEGMENTS = "total_segments"
    ATTRIBUTE_VALUE_LIST = "attribute_value_list"
    COMPARISON_OPERATOR = "comparison_operator"

    KEY_CONDITIONS = "key_conditions"
    SCAN_INDEX_FORWARD = "scan_index_forward"
    SELECT = "select"

    COUNT = "count"
    SCANNED_COUNT = "scanned_count"
    ITEMS = "items"
    LAST_EVALUATED_KEY = "last_evaluated_key"

    ATTRIBUTE_UPDATES = "attribute_updates"
    ACTION = "action"

    LINKS = "links"
    HREF = "href"
    REL = "rel"

    REQUEST_ITEMS = "request_items"
    REQUEST_DELETE = "delete_request"
    REQUEST_PUT = "put_request"


class Values():
    KEY_TYPE_HASH = "HASH"
    KEY_TYPE_RANGE = "RANGE"

    PROJECTION_TYPE_KEYS_ONLY = "KEYS_ONLY"
    PROJECTION_TYPE_INCLUDE = "INCLUDE"
    PROJECTION_TYPE_ALL = "ALL"

    ACTION_TYPE_PUT = UpdateItemAction.UPDATE_ACTION_PUT
    ACTION_TYPE_ADD = UpdateItemAction.UPDATE_ACTION_ADD
    ACTION_TYPE_DELETE = UpdateItemAction.UPDATE_ACTION_DELETE

    TABLE_STATUS_ACTIVE = TableMeta.TABLE_STATUS_ACTIVE
    TABLE_STATUS_CREATING = TableMeta.TABLE_STATUS_CREATING
    TABLE_STATUS_DELETING = TableMeta.TABLE_STATUS_DELETING

    RETURN_VALUES_NONE = "NONE"
    RETURN_VALUES_ALL_OLD = "ALL_OLD"
    RETURN_VALUES_UPDATED_OLD = "UPDATED_OLD"
    RETURN_VALUES_ALL_NEW = "ALL_NEW"
    RETURN_VALUES_UPDATED_NEW = "UPDATED_NEW"

    ALL_ATTRIBUTES = SelectType.SELECT_TYPE_ALL
    ALL_PROJECTED_ATTRIBUTES = SelectType.SELECT_TYPE_ALL_PROJECTED
    SPECIFIC_ATTRIBUTES = SelectType.SELECT_TYPE_SPECIFIC
    COUNT = SelectType.SELECT_TYPE_COUNT

    EQ = Condition.CONDITION_TYPE_EQUAL

    LE = IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL
    LT = IndexedCondition.CONDITION_TYPE_LESS
    GE = IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL
    GT = IndexedCondition.CONDITION_TYPE_GREATER
    BEGINS_WITH = "BEGINS_WITH"
    BETWEEN = "BETWEEN"

    NE = ScanCondition.CONDITION_TYPE_NOT_EQUAL
    CONTAINS = ScanCondition.CONDITION_TYPE_CONTAINS
    NOT_CONTAINS = ScanCondition.CONDITION_TYPE_NOT_CONTAINS
    IN = ScanCondition.CONDITION_TYPE_IN

    NOT_NULL = ScanCondition.CONDITION_TYPE_NOT_NULL
    NULL = ScanCondition.CONDITION_TYPE_NULL

    BOOKMARK = "bookmark"
    SELF = "self"


ATTRIBUTE_NAME_PATTERN = "^\w+"
TABLE_NAME_PATTERN = "^\w+"
INDEX_NAME_PATTERN = "^\w+"


class Types():
    ATTRIBUTE_NAME = {
        "type": "string",
        "pattern": ATTRIBUTE_NAME_PATTERN
    }

    TYPED_ATTRIBUTE_VALUE = {
        "type": "object"
    }

    ATTRIBUTE = {
        "type": "object",
        "maxProperties": 1,
        "patternProperties": {
            ATTRIBUTE_NAME_PATTERN: TYPED_ATTRIBUTE_VALUE
        }
    }

    ITEM = {
        "type": "object",
        "patternProperties": {
            ATTRIBUTE_NAME_PATTERN: TYPED_ATTRIBUTE_VALUE
        }
    }

    ATTRIBUTE_DEFINITION = {
        "type": "object",
        "required": [Props.ATTRIBUTE_NAME, Props.ATTRIBUTE_TYPE],
        "properties": {
            Props.ATTRIBUTE_NAME: ATTRIBUTE_NAME,
            Props.ATTRIBUTE_TYPE: {
                "type": "string"
            }
        }
    }

    ACTION_TYPE = {
        "type": "string",
        "enum": [Values.ACTION_TYPE_PUT,
                 Values.ACTION_TYPE_ADD,
                 Values.ACTION_TYPE_DELETE]
    }

    INDEX_NAME = {
        "type": "string",
        "pattern": INDEX_NAME_PATTERN
    }

    TABLE_NAME = {
        "type": "string",
        "pattern": TABLE_NAME_PATTERN,
    }

    KEY_SCHEMA = {
        "type": "array",
        "items": {
            "type": "object",
            "required": [Props.ATTRIBUTE_NAME, Props.KEY_TYPE],
            "properties": {
                Props.ATTRIBUTE_NAME: ATTRIBUTE_NAME,
                Props.KEY_TYPE: {
                    "type": "string"
                }
            }
        }
    }

    PROJECTION_TYPE = {
        "type": "string",
        "enum": [Values.PROJECTION_TYPE_KEYS_ONLY,
                 Values.PROJECTION_TYPE_INCLUDE,
                 Values.PROJECTION_TYPE_ALL]
    }

    PROJECTION = {
        "type": "object",
        "properties": {
            Props.NON_KEY_ATTRIBUTES: {
                "type": "array",
                "items": {
                    "type": "string",
                    "pattern": ATTRIBUTE_NAME_PATTERN
                }
            },
            Props.PROJECTION_TYPE: PROJECTION_TYPE
        }
    }

    SELECT = {
        "type": "string",
        "enum": [Values.ALL_ATTRIBUTES,
                 Values.ALL_PROJECTED_ATTRIBUTES,
                 Values.SPECIFIC_ATTRIBUTES,
                 Values.COUNT]
    }

    SCAN_OPERATOR = {
        "type": "string",
        "enum": [Values.EQ,
                 Values.NE,
                 Values.LE,
                 Values.LT,
                 Values.GE,
                 Values.GT,
                 Values.NOT_NULL,
                 Values.NULL,
                 Values.CONTAINS,
                 Values.NOT_CONTAINS,
                 Values.BEGINS_WITH,
                 Values.IN,
                 Values.BETWEEN]
    }

    QUERY_OPERATOR = {
        "type": "string",
        "enum": [Values.EQ,
                 Values.LE,
                 Values.LT,
                 Values.GE,
                 Values.GT,
                 Values.BEGINS_WITH,
                 Values.BETWEEN]
    }


class Parser():
    @classmethod
    def parse_attribute_definition(cls, attr_def_json):
        attr_name_json = attr_def_json.get(Props.ATTRIBUTE_NAME, None)
        attr_type_json = attr_def_json.get(Props.ATTRIBUTE_TYPE, "")

        storage_type = AttributeType(attr_type_json)

        return attr_name_json, storage_type

    @classmethod
    def format_attribute_definition(cls, attr_name, attr_type):
        type_json = attr_type.type

        return {
            Props.ATTRIBUTE_NAME: attr_name,
            Props.ATTRIBUTE_TYPE: type_json
        }

    @classmethod
    def parse_attribute_definitions(cls, attr_def_list_json):
        res = {}

        for attr_def_json in attr_def_list_json:
            attr_name, attr_type = (
                cls.parse_attribute_definition(attr_def_json)
            )
            res[attr_name] = attr_type

        return res

    @classmethod
    def format_attribute_definitions(cls, attr_def_map):
        return [
            cls.format_attribute_definition(attr_name, attr_type)
            for attr_name, attr_type in attr_def_map.iteritems()
        ]

    @classmethod
    def parse_key_schema(cls, key_def_list_json):
        hash_key_attr_name = None
        range_key_attr_name = None

        for key_def in key_def_list_json:
            key_attr_name_json = key_def.get(Props.ATTRIBUTE_NAME, None)
            key_type_json = key_def.get(Props.KEY_TYPE, None)

            if key_type_json == Values.KEY_TYPE_HASH:
                if hash_key_attr_name is not None:
                    raise ValidationError("Only one 'HASH' key is allowed")
                hash_key_attr_name = key_attr_name_json
            elif key_type_json == Values.KEY_TYPE_RANGE:
                if range_key_attr_name is not None:
                    raise ValidationError("Only one 'RANGE' key is allowed")
                range_key_attr_name = key_attr_name_json
            else:
                raise ValidationError(
                    "Only 'RANGE' or 'HASH' key types are allowed, but "
                    "'%(key_type)s' is found", key_type=key_type_json)
        if hash_key_attr_name is None:
            raise ValidationError("HASH key is missing")
        if range_key_attr_name:
            return (hash_key_attr_name, range_key_attr_name)
        return (hash_key_attr_name,)

    @classmethod
    def format_key_schema(cls, key_attr_names):
        assert len(key_attr_names) > 0, (
            "At least HASH key should be specified. No one is given"
        )

        assert len(key_attr_names) <= 2, (
            "More then 2 keys given. Only one HASH and one RANGE key allowed"
        )

        res = [
            {
                Props.KEY_TYPE: Values.KEY_TYPE_HASH,
                Props.ATTRIBUTE_NAME: key_attr_names[0]
            }
        ]

        if len(key_attr_names) > 1:
            res.append({
                Props.KEY_TYPE: Values.KEY_TYPE_RANGE,
                Props.ATTRIBUTE_NAME: key_attr_names[1]
            })

        return res

    @classmethod
    def parse_local_secondary_index(cls, local_secondary_index_json):
        key_attrs_for_projection = cls.parse_key_schema(
            local_secondary_index_json.get(Props.KEY_SCHEMA, {})
        )

        try:
            range_key = key_attrs_for_projection[1]
        except IndexError:
            raise ValidationError("Range key in index wasn't specified")

        index_name = local_secondary_index_json[Props.INDEX_NAME]

        projection_type = local_secondary_index_json.get(
            Props.PROJECTION_TYPE, Values.PROJECTION_TYPE_INCLUDE
        )

        if projection_type == Values.PROJECTION_TYPE_ALL:
            projected_attrs = None
        elif projection_type == Values.PROJECTION_TYPE_KEYS_ONLY:
            projected_attrs = tuple()
        else:
            projected_attrs = local_secondary_index_json.get(
                Props.NON_KEY_ATTRIBUTES, None
            )

        return index_name, IndexDefinition(range_key, projected_attrs)

    @classmethod
    def format_local_secondary_index(cls, index_name, hash_key,
                                     local_secondary_index):
        if local_secondary_index.projected_attributes:
            projection = {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_INCLUDE,
                Props.NON_KEY_ATTRIBUTES: list(
                    local_secondary_index.projected_attributes
                )
            }
        elif local_secondary_index.projected_attributes is None:
            projection = {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_ALL
            }
        else:
            projection = {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_KEYS_ONLY
            }

        return {
            Props.INDEX_NAME: index_name,
            Props.KEY_SCHEMA: cls.format_key_schema(
                (hash_key, local_secondary_index.attribute_to_index)
            ),
            Props.PROJECTION: projection,
            Props.ITEM_COUNT: 0,
            Props.INDEX_SIZE_BYTES: 0
        }

    @classmethod
    def parse_local_secondary_indexes(cls, local_secondary_index_list_json):
        res = {}

        for index_json in local_secondary_index_list_json:
            index_name, index_def = (
                cls.parse_local_secondary_index(index_json)
            )
            res[index_name] = index_def

        return res

    @classmethod
    def format_local_secondary_indexes(cls, hash_key,
                                       local_secondary_index_map):
        return [
            cls.format_local_secondary_index(index_name, hash_key, index_def)
            for index_name, index_def in local_secondary_index_map.iteritems()
        ]

    @classmethod
    def encode_attr_value(cls, attr_value):
        return {
            attr_value.attr_type.type: attr_value.encoded_value
        }

    @classmethod
    def parse_typed_attr_value(cls, typed_attr_value_json):
        if len(typed_attr_value_json) != 1:
            raise ValidationError(
                "Can't recognize attribute format ['%(attr)s']",
                attr=json.dumps(typed_attr_value_json)
            )
        (attr_type_json, attr_value_json) = typed_attr_value_json.items()[0]

        return AttributeValue(attr_type_json, attr_value_json)

    @classmethod
    def parse_item_attributes(cls, item_attributes_json):
        item = {}
        for (attr_name_json, typed_attr_value_json) in (
                item_attributes_json.iteritems()):
            item[attr_name_json] = cls.parse_typed_attr_value(
                typed_attr_value_json
            )

        return item

    @classmethod
    def format_item_attributes(cls, item_attributes):
        attributes_json = {}
        for (attr_name, attr_value) in item_attributes.iteritems():
            attributes_json[attr_name] = cls.encode_attr_value(attr_value)

        return attributes_json

    @classmethod
    def parse_expected_attribute_conditions(
            cls, expected_attribute_conditions_json):
        expected_attribute_conditions = {}

        for (attr_name_json, condition_json) in (
                expected_attribute_conditions_json.iteritems()):

            if Props.VALUE in condition_json:
                expected_attribute_conditions[attr_name_json] = [
                    ExpectedCondition.eq(
                        cls.parse_typed_attr_value(condition_json[Props.VALUE])
                    )
                ]

            elif Props.EXISTS in condition_json:
                condition_value_json = condition_json[Props.EXISTS]

                assert isinstance(condition_value_json, bool)

                expected_attribute_conditions[attr_name_json] = [
                    ExpectedCondition.not_null()
                    if condition_value_json else
                    ExpectedCondition.null()
                ]

        return expected_attribute_conditions

    @classmethod
    def parse_select_type(cls, select, attributes_to_get,
                          select_on_index=False):
        if select is None:
            if attributes_to_get:
                return SelectType.specific_attributes(
                    attributes_to_get
                )
            else:
                if select_on_index:
                    return SelectType.all_projected()
                else:
                    return SelectType.all()

        if select == Values.SPECIFIC_ATTRIBUTES:
            assert attributes_to_get
            return SelectType.specific_attributes(attributes_to_get)

        assert not attributes_to_get

        if select == Values.ALL_ATTRIBUTES:
            return SelectType.all()

        if select == Values.ALL_PROJECTED_ATTRIBUTES:
            assert select_on_index
            return SelectType.all_projected()

        if select == Values.COUNT:
            return SelectType.count()

        assert False, "Select type wasn't recognized"

    SINGLE_ARGUMENT_CONDITIONS = {
        Values.EQ, Values.GT, Values.GE, Values.LT, Values.LE,
        Values.BEGINS_WITH, Values.NE, Values.CONTAINS, Values.NOT_CONTAINS
    }

    @classmethod
    def parse_attribute_condition(cls, condition_type, condition_args,
                                  condition_class=IndexedCondition):

        actual_args_count = (
            len(condition_args) if condition_args is not None else 0
        )
        if condition_type == Values.BETWEEN:
            if actual_args_count != 2:
                raise ValidationError(
                    "%(type)s condition type requires exactly 2 arguments, "
                    "but %(actual_args_count)s given",
                    type=condition_type,
                    actual_args_count=actual_args_count
                )
            if condition_args[0].attr_type != condition_args[1].attr_type:
                raise ValidationError(
                    "%(type)s condition type requires arguments of the "
                    "same type, but different types given",
                    type=condition_type,
                )

            return [
                condition_class.ge(condition_args[0]),
                condition_class.le(condition_args[1])
            ]

        if condition_type == Values.BEGINS_WITH:
            first = condition_class(
                condition_class.CONDITION_TYPE_GREATER_OR_EQUAL,
                condition_args
            )
            condition_arg = first.arg
            second = condition_class.le(
                AttributeValue(
                    condition_arg.attr_type, decoded_value=(
                        condition_arg.decoded_value[:-1] +
                        chr(ord(condition_arg.decoded_value[-1]) + 1)
                    )
                )
            )

            return [first, second]

        return [condition_class(condition_type, condition_args)]

    @classmethod
    def parse_attribute_conditions(cls, attribute_conditions_json,
                                   condition_class=IndexedCondition):
        attribute_conditions_json = attribute_conditions_json or {}

        attribute_conditions = {}

        for (attr_name, condition_json) in (
                attribute_conditions_json.iteritems()):
            condition_type_json = (
                condition_json[Props.COMPARISON_OPERATOR]
            )

            condition_args = map(
                cls.parse_typed_attr_value,
                condition_json.get(Props.ATTRIBUTE_VALUE_LIST, {})
            )

            attribute_conditions[attr_name] = (
                cls.parse_attribute_condition(
                    condition_type_json, condition_args, condition_class
                )
            )

        return attribute_conditions

    @classmethod
    def parse_attribute_updates(cls, attribute_updates_json):
        attribute_updates = {}
        attribute_updates_json = attribute_updates_json or {}

        for attr, attr_update_json in attribute_updates_json.iteritems():
            action_type_json = attr_update_json[Props.ACTION]

            value_json = attr_update_json.get(Props.VALUE)

            value = None
            if value_json:
                assert len(value_json) == 1
                (attr_type_json, attr_value_json) = (
                    value_json.items()[0]
                )

                value = AttributeValue(attr_type_json, attr_value_json)

            update_action = UpdateItemAction(action_type_json, value)

            attribute_updates[attr] = update_action

        return attribute_updates

    @classmethod
    def parse_request_items(cls, request_items_json):
        for table_name, request_list in request_items_json.iteritems():
            for request in request_list:
                for request_type, request_body in request.iteritems():
                    if request_type == Props.REQUEST_PUT:
                        yield PutItemRequest(
                            table_name,
                            cls.parse_item_attributes(
                                request_body[Props.ITEM]))
                    elif request_type == Props.REQUEST_DELETE:
                        yield DeleteItemRequest(
                            table_name,
                            cls.parse_item_attributes(
                                request_body[Props.KEY]))

    @classmethod
    def parse_batch_get_request_items(cls, request_items_json):
        for table_name, request_body in request_items_json.iteritems():
            attributes_to_get = request_body.get(Props.ATTRIBUTES_TO_GET)
            consistent = request_body.get(Props.CONSISTENT_READ, False)
            select_type = (
                SelectType.all()
                if attributes_to_get is None else
                SelectType.specific_attributes(attributes_to_get)
            )
            for key in request_body[Props.KEYS]:
                key_attr = cls.parse_item_attributes(key)
                indexed_condition_map = {
                    name: [IndexedCondition.eq(value)]
                    for name, value in key_attr.iteritems()
                }
                yield GetItemRequest(
                    table_name,
                    indexed_condition_map,
                    select_type=select_type,
                    consistent=consistent)

    @classmethod
    def format_request_items(cls, request_items):
        res = {}
        for request in request_items:
            table_requests = res.get(request.table_name, None)
            if table_requests is None:
                table_requests = []
                res[request.table_name] = table_requests

            if isinstance(request, PutItemRequest):
                request_json = {
                    Props.REQUEST_PUT: {
                        Props.ITEM: cls.format_item_attributes(
                            request.attribute_map)
                    }

                }
            elif isinstance(request, DeleteItemRequest):
                request_json = {
                    Props.REQUEST_DELETE: {
                        Props.KEY: cls.format_item_attributes(
                            request.key_attribute_map)
                    }
                }
            else:
                assert False, (
                    "Unknown request type '{}'".format(
                        request.__class__.__name__
                    )
                )

            table_requests.append(request_json)

        return res

    @classmethod
    def format_batch_get_unprocessed(cls, unprocessed, request_items):
        res = {}
        for request in unprocessed:
            tname = request.table_name
            table_res = res.get(request.table_name, None)
            if table_res is None:
                table_res = {Props.KEYS: []}
                res[tname] = table_res
            attr_map = {}
            for key, value in request.indexed_condition_map.iteritems():
                attr_map[key] = value[0].arg
            table_res[Props.KEYS].append(cls.format_item_attributes(attr_map))
            attr_to_get = request_items[tname].get(Props.ATTRIBUTES_TO_GET)
            consistent = request_items[tname].get(Props.CONSISTENT_READ)
            if attr_to_get:
                table_res[Props.ATTRIBUTES_TO_GET] = attr_to_get
            if consistent:
                table_res[Props.CONSISTENT_READ] = consistent
        return res

    @classmethod
    def format_table_status(cls, table_status):
        if table_status == TableMeta.TABLE_STATUS_ACTIVE:
            return Values.TABLE_STATUS_ACTIVE
        elif table_status == TableMeta.TABLE_STATUS_CREATING:
            return Values.TABLE_STATUS_CREATING
        elif table_status == TableMeta.TABLE_STATUS_DELETING:
            return Values.TABLE_STATUS_DELETING
        else:
            assert False, (
                "Table status '{}' is not allowed".format(table_status)
            )
