# Copyright 2014 Mirantis Inc.
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
import base64
import decimal

from magnetodb.storage import models
from magnetodb.storage.models import IndexDefinition
from magnetodb.common.exception import MagnetoError


# init decimal context to meet DynamoDB number type behaviour expectation
DECIMAL_CONTEXT = decimal.Context(
    prec=38, rounding=None,
    traps=[],
    flags=[],
    Emax=126,
    Emin=-128
)

TYPE_STRING = "S"
TYPE_NUMBER = "N"
TYPE_BLOB = "B"
TYPE_STRING_SET = "SS"
TYPE_NUMBER_SET = "NS"
TYPE_BLOB_SET = "BS"


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
    ITEM_TYPE_STRING = TYPE_STRING
    ITEM_TYPE_NUMBER = TYPE_NUMBER
    ITEM_TYPE_BLOB = TYPE_BLOB
    ITEM_TYPE_STRING_SET = TYPE_STRING_SET
    ITEM_TYPE_NUMBER_SET = TYPE_NUMBER_SET
    ITEM_TYPE_BLOB_SET = TYPE_BLOB_SET
    ITEM = "item"
    RETURN_VALUES = "return_values"

    ATTRIBUTES = "attributes"
    ITEM_COLLECTION_METRICS = "item_collection_metrics"
    ITEM_COLLECTION_KEY = "item_collection_key"

    ATTRIBUTES_TO_GET = "attributes_to_get"
    CONSISTENT_READ = "consistent_read"
    KEY = "key"

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
    ATTRIBUTE_TYPE_STRING = TYPE_STRING
    ATTRIBUTE_TYPE_NUMBER = TYPE_NUMBER
    ATTRIBUTE_TYPE_BLOB = TYPE_BLOB
    ATTRIBUTE_TYPE_STRING_SET = TYPE_STRING_SET
    ATTRIBUTE_TYPE_NUMBER_SET = TYPE_NUMBER_SET
    ATTRIBUTE_TYPE_BLOB_SET = TYPE_BLOB_SET

    KEY_TYPE_HASH = "HASH"
    KEY_TYPE_RANGE = "RANGE"

    PROJECTION_TYPE_KEYS_ONLY = "KEYS_ONLY"
    PROJECTION_TYPE_INCLUDE = "INCLUDE"
    PROJECTION_TYPE_ALL = "ALL"

    ACTION_TYPE_PUT = "PUT"
    ACTION_TYPE_ADD = "ADD"
    ACTION_TYPE_DELETE = "DELETE"

    TABLE_STATUS_ACTIVE = "ACTIVE"
    TABLE_STATUS_CREATING = "CREATING"
    TABLE_STATUS_DELETING = "DELETING"

    RETURN_CONSUMED_CAPACITY_INDEXES = "INDEXES"
    RETURN_CONSUMED_CAPACITY_TOTAL = "TOTAL"
    RETURN_CONSUMED_CAPACITY_NONE = "NONE"

    RETURN_ITEM_COLLECTION_METRICS_SIZE = "SIZE"
    RETURN_ITEM_COLLECTION_METRICS_NONE = "NONE"

    RETURN_VALUES_NONE = "NONE"
    RETURN_VALUES_ALL_OLD = "ALL_OLD"
    RETURN_VALUES_UPDATED_OLD = "UPDATED_OLD"
    RETURN_VALUES_ALL_NEW = "ALL_NEW"
    RETURN_VALUES_UPDATED_NEW = "UPDATED_NEW"

    ALL_ATTRIBUTES = "ALL_ATTRIBUTES"
    ALL_PROJECTED_ATTRIBUTES = "ALL_PROJECTED_ATTRIBUTES"
    SPECIFIC_ATTRIBUTES = "SPECIFIC_ATTRIBUTES"
    COUNT = "COUNT"

    EQ = "EQ"
    NE = "NE"
    LE = "LE"
    LT = "LT"
    GE = "GE"
    GT = "GT"
    NOT_NULL = "NOT_NULL"
    NULL = "NULL"
    CONTAINS = "CONTAINS"
    NOT_CONTAINS = "NOT_CONTAINS"
    BEGINS_WITH = "BEGINS_WITH"
    IN = "IN"
    BETWEEN = "BETWEEN"

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

    ATTRIBUTE_TYPE = {
        "type": "string",
        "enum": [Values.ATTRIBUTE_TYPE_STRING,
                 Values.ATTRIBUTE_TYPE_NUMBER,
                 Values.ATTRIBUTE_TYPE_BLOB,
                 Values.ATTRIBUTE_TYPE_STRING_SET,
                 Values.ATTRIBUTE_TYPE_NUMBER_SET,
                 Values.ATTRIBUTE_TYPE_BLOB_SET]
    }

    ATTRIBUTE_DEFINITION = {
        "type": "object",
        "required": [Props.ATTRIBUTE_NAME, Props.ATTRIBUTE_TYPE],
        "properties": {
            Props.ATTRIBUTE_NAME: ATTRIBUTE_NAME,
            Props.ATTRIBUTE_TYPE: ATTRIBUTE_TYPE
        }
    }

    KEY_TYPE = {
        "type": "string",
        "enum": [Values.KEY_TYPE_HASH, Values.KEY_TYPE_RANGE]
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
                Props.KEY_TYPE: KEY_TYPE
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

    ITEM_VALUE = {
        "oneOf": [
            {
                "type": "object",
                "required": [Props.ITEM_TYPE_STRING],
                "properties": {
                    Props.ITEM_TYPE_STRING: {
                        "type": "string"
                    }
                }
            },
            {
                "type": "object",
                "required": [Props.ITEM_TYPE_NUMBER],
                "properties": {
                    Props.ITEM_TYPE_NUMBER: {
                        "type": "string"
                    }
                }
            },
            {
                "type": "object",
                "required": [Props.ITEM_TYPE_BLOB],
                "properties": {
                    Props.ITEM_TYPE_BLOB: {
                        "type": "string"
                    }
                }
            },
            {
                "type": "object",
                "required": [Props.ITEM_TYPE_STRING_SET],
                "properties": {
                    Props.ITEM_TYPE_STRING_SET: {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            },
            {
                "type": "object",
                "required": [Props.ITEM_TYPE_NUMBER_SET],
                "properties": {
                    Props.ITEM_TYPE_NUMBER_SET: {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            },
            {
                "type": "object",
                "required": [Props.ITEM_TYPE_BLOB_SET],
                "properties": {
                    Props.ITEM_TYPE_BLOB_SET: {
                        "type": "array",
                        "items": {
                            "type": "string"
                        }
                    }
                }
            }
        ]
    }

    RETURN_CONSUMED_CAPACITY = {
        "type": "string",
        "enum": [Values.RETURN_CONSUMED_CAPACITY_INDEXES,
                 Values.RETURN_CONSUMED_CAPACITY_TOTAL,
                 Values.RETURN_CONSUMED_CAPACITY_NONE]
    }

    RETURN_ITEM_COLLECTION_METRICS = {
        "type": "string",
        "enum": [Values.RETURN_ITEM_COLLECTION_METRICS_SIZE,
                 Values.RETURN_ITEM_COLLECTION_METRICS_NONE]
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
    DYNAMODB_TO_STORAGE_TYPE_MAP = {
        Values.ATTRIBUTE_TYPE_STRING: models.ATTRIBUTE_TYPE_STRING,
        Values.ATTRIBUTE_TYPE_STRING_SET: models.ATTRIBUTE_TYPE_STRING_SET,
        Values.ATTRIBUTE_TYPE_NUMBER: models.ATTRIBUTE_TYPE_NUMBER,
        Values.ATTRIBUTE_TYPE_NUMBER_SET: models.ATTRIBUTE_TYPE_NUMBER_SET,
        Values.ATTRIBUTE_TYPE_BLOB: models.ATTRIBUTE_TYPE_BLOB,
        Values.ATTRIBUTE_TYPE_BLOB_SET: models.ATTRIBUTE_TYPE_BLOB_SET
    }

    STORAGE_TO_DYNAMODB_TYPE_MAP = {
        v: k for k, v in DYNAMODB_TO_STORAGE_TYPE_MAP.iteritems()
    }

    @classmethod
    def parse_attribute_definition(cls, attr_def_json):
        dynamodb_attr_name = attr_def_json.get(Props.ATTRIBUTE_NAME, None)
        dynamodb__attr_type = attr_def_json.get(Props.ATTRIBUTE_TYPE, "")

        storage_type = cls.DYNAMODB_TO_STORAGE_TYPE_MAP.get(
            dynamodb__attr_type, None
        )

        return dynamodb_attr_name, storage_type

    @classmethod
    def format_attribute_definition(cls, attr_name, attr_type):
        dynamodb_type = cls.STORAGE_TO_DYNAMODB_TYPE_MAP.get(attr_type,
                                                             None)

        assert dynamodb_type, (
            "Unknown Attribute type returned by backend: %s" % attr_type
        )

        return {
            Props.ATTRIBUTE_NAME: attr_name,
            Props.ATTRIBUTE_TYPE: dynamodb_type
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
            dynamodb_key_attr_name = key_def.get(Props.ATTRIBUTE_NAME, None)
            dynamodb_key_type = key_def.get(Props.KEY_TYPE, None)

            if dynamodb_key_type == Values.KEY_TYPE_HASH:
                assert hash_key_attr_name is None, (
                    "Only one HASH key is allowed"
                )

                hash_key_attr_name = dynamodb_key_attr_name
            elif dynamodb_key_type == Values.KEY_TYPE_RANGE:
                assert range_key_attr_name is None, (
                    "Only one RANGE key is allowed"
                )
                range_key_attr_name = dynamodb_key_attr_name

        if range_key_attr_name:
            return hash_key_attr_name, range_key_attr_name
        return hash_key_attr_name,

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
            raise MagnetoError("Range key in index wasn't specified")

        index_name = local_secondary_index_json[Props.INDEX_NAME]

        projection_type = local_secondary_index_json.get(
            Props.PROJECTION_TYPE, Values.PROJECTION_TYPE_INCLUDE
        )

        if projection_type == Values.PROJECTION_TYPE_ALL:
            projected_attrs = None
        elif projection_type == Values.PROJECTION_TYPE_KEYS_ONLY:
            projected_attrs = frozenset()
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

        for index__json in local_secondary_index_list_json:
            index_name, index_def = (
                cls.parse_local_secondary_index(index__json)
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

    @staticmethod
    def decode_single_value(single_value_type, encoded_single_value):
        assert isinstance(encoded_single_value, (str, unicode))
        if single_value_type == models.AttributeType.ELEMENT_TYPE_STRING:
            return encoded_single_value
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            return DECIMAL_CONTEXT.create_decimal(encoded_single_value)
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            return base64.decodestring(encoded_single_value)
        else:
            assert False, "Value type wasn't recognized"

    @staticmethod
    def encode_single_value(single_value_type, decoded_single_value):
        if single_value_type == models.AttributeType.ELEMENT_TYPE_STRING:
            isinstance(decoded_single_value, (str, unicode))
            return decoded_single_value
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            assert isinstance(decoded_single_value, decimal.Decimal)
            return str(decoded_single_value)
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            assert isinstance(decoded_single_value, str)
            return base64.encodestring(decoded_single_value)
        else:
            assert False, "Value type wasn't recognized"

    @classmethod
    def decode_attr_value(cls, dynamodb_attr_type, dynamodb_attr_value):
        attr_type = cls.DYNAMODB_TO_STORAGE_TYPE_MAP[dynamodb_attr_type]
        if attr_type.collection_type is not None:
            attr_value = {
                cls.decode_single_value(attr_type.element_type, val)
                for val in dynamodb_attr_value
            }
        else:
            attr_value = cls.decode_single_value(
                attr_type.element_type, dynamodb_attr_value
            )
        return models.AttributeValue(attr_type, attr_value)

    @classmethod
    def encode_attr_value(cls, attr_value):
        if attr_value.type.collection_type is not None:
            dynamodb_attr_value = map(
                lambda val: cls.encode_single_value(
                    attr_value.type.element_type,
                    val),
                attr_value.value
            )
        else:
            dynamodb_attr_value = cls.encode_single_value(
                attr_value.type.element_type, attr_value.value
            )

        return {
            cls.STORAGE_TO_DYNAMODB_TYPE_MAP[attr_value.type]:
            dynamodb_attr_value
        }

    @classmethod
    def parse_item_attributes(cls, item_attributes_json):
        item = {}
        for (attr_name, dynamodb_attr) in item_attributes_json.iteritems():
            assert len(dynamodb_attr) == 1
            (dynamodb_attr_type, dynamodb_attr_value) = (
                dynamodb_attr.items()[0]
            )
            item[attr_name] = cls.decode_attr_value(dynamodb_attr_type,
                                                    dynamodb_attr_value)

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

        for (attr_name, dynamodb_condition) in (
                expected_attribute_conditions_json.iteritems()):

            if Props.VALUE in dynamodb_condition:
                dynamodb_condition_value = dynamodb_condition[Props.VALUE]

                assert len(dynamodb_condition_value) == 1

                (dynamodb_attr_type, dynamodb_attr_value) = (
                    dynamodb_condition_value.items()[0]
                )
                expected_attribute_conditions[attr_name] = [
                    models.ExpectedCondition.eq(
                        cls.decode_attr_value(
                            dynamodb_attr_type, dynamodb_attr_value
                        )
                    )
                ]

            elif Props.EXISTS in dynamodb_condition:
                dynamodb_condition_value = dynamodb_condition[Props.EXISTS]

                assert isinstance(dynamodb_condition_value, bool)

                expected_attribute_conditions[attr_name] = [
                    models.ExpectedCondition.exists()
                    if dynamodb_condition_value else
                    models.ExpectedCondition.not_exists()
                ]

        return expected_attribute_conditions

    @staticmethod
    def format_consumed_capacity(return_consumed_capacity, table_schema):
        if return_consumed_capacity == Values.RETURN_CONSUMED_CAPACITY_NONE:
            return None

        consumed_capacity = {
            Props.GLOBAL_SECONDARY_INDEXES: {
                # TODO(dukhlov):
                # read schema and fill global index consumed
                # capacity to imitate DynamoDB API
                "global_index_name": {
                    Props.CAPACITY_UNITS: 0
                }
            },
            Props.LOCAL_SECONDARY_INDEXES: {
                # TODO(dukhlov):
                # read schema and fill local index consumed
                # capacity to imitate DynamoDB API
                "local_index_name": {
                    Props.CAPACITY_UNITS: 0
                }
            }
        }

        if return_consumed_capacity == Values.RETURN_CONSUMED_CAPACITY_TOTAL:
            consumed_capacity[Props.CAPACITY_UNITS] = 0
            consumed_capacity[Props.TABLE] = {
                Props.CAPACITY_UNITS: 0
            }

        return consumed_capacity

    @classmethod
    def parse_select_type(cls, select, attributes_to_get,
                          select_on_index=False):
        if select is None:
            if attributes_to_get:
                return models.SelectType.specified_attributes(
                    attributes_to_get
                )
            else:
                if select_on_index:
                    return models.SelectType.all_projected()
                else:
                    return models.SelectType.all()

        if select == Values.SPECIFIC_ATTRIBUTES:
            assert attributes_to_get
            return models.SelectType.specified_attributes(attributes_to_get)

        assert not attributes_to_get

        if select == Values.ALL_ATTRIBUTES:
            return models.SelectType.all()

        if select == Values.ALL_PROJECTED_ATTRIBUTES:
            assert select_on_index
            return models.SelectType.all_projected()

        if select == Values.COUNT:
            return models.SelectType.count()

        assert False, "Select type wasn't recognized"

    @classmethod
    def parse_attribute_conditions(
            cls, attribute_conditions_json):
        attribute_conditions = {}

        attribute_conditions_json = attribute_conditions_json or {}

        for (attr_name, dynamodb_condition) in (
                attribute_conditions_json.iteritems()):
            dynamodb_condition_type = (
                dynamodb_condition[Props.COMPARISON_OPERATOR]
            )
            condition_args = map(
                lambda attr_value: cls.decode_attr_value(
                    *attr_value.items()[0]),
                dynamodb_condition.get(Props.ATTRIBUTE_VALUE_LIST, {})
            )

            if dynamodb_condition_type == Values.EQ:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.IndexedCondition.eq(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.GT:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.IndexedCondition.gt(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.LT:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.IndexedCondition.lt(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.GE:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.IndexedCondition.ge(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.LE:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.IndexedCondition.le(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.BEGINS_WITH:
                assert len(condition_args) == 1

                first = condition_args[0]
                second = models.AttributeValue(
                    first.type,
                    first.value[:-1] + chr(ord(first.value[-1]) + 1)
                )

                attribute_conditions[attr_name] = [
                    models.IndexedCondition.ge(first),
                    models.IndexedCondition.lt(second)
                ]
            elif dynamodb_condition_type == Values.BETWEEN:
                assert len(condition_args) == 2
                assert condition_args[0].type == condition_args[1].type

                attribute_conditions[attr_name] = [
                    models.IndexedCondition.ge(condition_args[0]),
                    models.IndexedCondition.le(condition_args[1])
                ]
            elif dynamodb_condition_type == Values.NE:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.ScanCondition.neq(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.CONTAINS:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.ScanCondition.contains(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.NOT_CONTAINS:
                assert len(condition_args) == 1
                attribute_conditions[attr_name] = [
                    models.ScanCondition.not_contains(condition_args[0])
                ]
            elif dynamodb_condition_type == Values.IN:
                attribute_conditions[attr_name] = [
                    models.ScanCondition.in_set(condition_args)
                ]
            elif dynamodb_condition_type == Values.NULL:
                attribute_conditions[attr_name] = [
                    models.ExpectedCondition.not_exists()
                ]
            elif dynamodb_condition_type == Values.NOT_NULL:
                attribute_conditions[attr_name] = [
                    models.ExpectedCondition.exists()
                ]

        return attribute_conditions

    @classmethod
    def parse_attribute_updates(cls, attribute_updates_json):
        attribute_updates = {}
        attribute_updates_json = attribute_updates_json or {}

        for attr, attr_update_json in attribute_updates_json.iteritems():
            action_type = attr_update_json[Props.ACTION]

            if action_type == Values.ACTION_TYPE_ADD:
                action = models.UpdateItemAction.UPDATE_ACTION_ADD
            elif action_type == Values.ACTION_TYPE_DELETE:
                action = models.UpdateItemAction.UPDATE_ACTION_DELETE
            elif action_type == Values.ACTION_TYPE_PUT:
                action = models.UpdateItemAction.UPDATE_ACTION_PUT

            dynamodb_value = attr_update_json[Props.VALUE]

            assert len(dynamodb_value) == 1
            (dynamodb_attr_type, dynamodb_attr_value) = (
                dynamodb_value.items()[0]
            )

            value = cls.decode_attr_value(
                dynamodb_attr_type, dynamodb_attr_value)

            update_action = models.UpdateItemAction(action, value)

            attribute_updates[attr] = update_action

        return attribute_updates

    @classmethod
    def parse_request_items(cls, request_items_json):
        for table_name, request_list in request_items_json.iteritems():
            for request in request_list:
                for request_type, request_body in request.iteritems():
                    if request_type == Props.REQUEST_PUT:
                        yield models.PutItemRequest(
                            table_name,
                            cls.parse_item_attributes(
                                request_body[Props.ITEM]))
                    elif request_type == Props.REQUEST_DELETE:
                        yield models.DeleteItemRequest(
                            table_name,
                            cls.parse_item_attributes(
                                request_body[Props.KEY]))

    @classmethod
    def format_request_items(cls, request_items):
        res = {}
        for request in request_items:
            table_requests = res.get(request.table_name, None)
            if table_requests is None:
                table_requests = []
                res[request.table_name] = table_requests

            if isinstance(request, models.PutItemRequest):
                request_json = {
                    Props.REQUEST_PUT: {
                        Props.ITEM: cls.format_item_attributes(
                            request.attribute_map)
                    }

                }
            elif isinstance(request, models.DeleteItemRequest):
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
    def format_table_status(cls, table_status):
        if table_status == models.TableMeta.TABLE_STATUS_ACTIVE:
            return Values.TABLE_STATUS_ACTIVE
        elif table_status == models.TableMeta.TABLE_STATUS_CREATING:
            return Values.TABLE_STATUS_CREATING
        elif table_status == models.TableMeta.TABLE_STATUS_DELETING:
            return Values.TABLE_STATUS_DELETING
        else:
            assert False, (
                "Table status '{}' is not allowed".format(table_status)
            )
