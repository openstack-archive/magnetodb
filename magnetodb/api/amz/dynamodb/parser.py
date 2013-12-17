# vim: tabstop=4 shiftwidth=4 softtabstop=4

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
from __builtin__ import isinstance
import decimal

from magnetodb.storage import models
from magnetodb.storage.models import IndexDefinition
from magnetodb.common.exception import MagnetoError


#init decimal context to meet DynamoDB number type behaviour expectation
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
    TABLE_NAME = "TableName"
    ATTRIBUTE_DEFINITIONS = "AttributeDefinitions"
    ATTRIBUTE_NAME = "AttributeName"
    ATTRIBUTE_TYPE = "AttributeType"
    KEY_SCHEMA = "KeySchema"
    KEY_TYPE = "KeyType"
    LOCAL_SECONDARY_INDEXES = "LocalSecondaryIndexes"
    GLOBAL_SECONDARY_INDEXES = "GlobalSecondaryIndexes"
    INDEX_NAME = "IndexName"
    PROJECTION = "Projection"
    NON_KEY_ATTRIBUTES = "NonKeyAttributes"
    PROJECTION_TYPE = "ProjectionType"
    PROVISIONED_THROUGHPUT = "ProvisionedThroughput"
    READ_CAPACITY_UNITS = "ReadCapacityUnits"
    WRITE_CAPACITY_UNITS = "WriteCapacityUnits"

    TABLE_DESCRIPTION = "TableDescription"
    TABLE = "Table"
    TABLE_SIZE_BYTES = "TableSizeBytes"
    TABLE_STATUS = "TableStatus"
    CREATION_DATE_TIME = "CreationDateTime"
    INDEX_SIZE_BYTES = "IndexSizeBytes"
    ITEM_COUNT = "ItemCount"

    TABLE_NAMES = "TableNames"
    EXCLUSIVE_START_TABLE_NAME = "ExclusiveStartTableName"
    LAST_EVALUATED_TABLE_NAME = "LastEvaluatedTableName"
    LIMIT = "Limit"

    EXPECTED = "Expected"
    EXISTS = "Exists"
    VALUE = "Value"
    ITEM_TYPE_STRING = TYPE_STRING
    ITEM_TYPE_NUMBER = TYPE_NUMBER
    ITEM_TYPE_BLOB = TYPE_BLOB
    ITEM_TYPE_STRING_SET = TYPE_STRING_SET
    ITEM_TYPE_NUMBER_SET = TYPE_NUMBER_SET
    ITEM_TYPE_BLOB_SET = TYPE_BLOB_SET
    ITEM = "Item"
    RETURN_CONSUMED_CAPACITY = "ReturnConsumedCapacity"
    RETURN_ITEM_COLLECTION_METRICS = "ReturnItemCollectionMetrics"
    RETURN_VALUES = "ReturnValues"

    ATTRIBUTES = "Attributes"
    ITEM_COLLECTION_METRICS = "ItemCollectionMetrics"
    ITEM_COLLECTION_KEY = "ItemCollectionKey"
    SIZE_ESTIMATED_RANGE_GB = "SizeEstimateRangeGB"
    CONSUMED_CAPACITY = "ConsumedCapacity"
    CAPACITY_UNITS = "CapacityUnits"


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

    PROVISIONED_THROUGHPUT_DUMMY = {
        "LastDecreaseDateTime": 0,
        "LastIncreaseDateTime": 0,
        "NumberOfDecreasesToday": 0,
        "ReadCapacityUnits": 0,
        "WriteCapacityUnits": 0
    }

    TABLE_STATUS_ACTIVE = "ACTIVE"

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

    PROVISIONED_THROUGHPUT = {
        "type": "object",
        "required": [Props.READ_CAPACITY_UNITS, Props.WRITE_CAPACITY_UNITS],
        "properties": {
            Props.READ_CAPACITY_UNITS: {
                "type": "integer"
            },
            Props.WRITE_CAPACITY_UNITS: {
                "type": "integer"
            }
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

        return models.AttributeDefinition(dynamodb_attr_name, storage_type)

    @classmethod
    def format_attribute_definition(cls, attr_def):
        dynamodb_type = cls.STORAGE_TO_DYNAMODB_TYPE_MAP.get(attr_def.type,
                                                             None)

        assert dynamodb_type, (
            "Unknown Attribute type returned by backend: %s" % attr_def.type
        )

        return {
            Props.ATTRIBUTE_NAME: attr_def.name,
            Props.ATTRIBUTE_TYPE: dynamodb_type
        }

    @classmethod
    def parse_attribute_definitions(cls, attr_def_list_json):
        return map(cls.parse_attribute_definition, attr_def_list_json)

    @classmethod
    def format_attribute_definitions(cls, attr_def_list):
        return map(cls.format_attribute_definition, attr_def_list)

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

        return (hash_key_attr_name, range_key_attr_name)

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

        return IndexDefinition(index_name, range_key, projected_attrs)

    @classmethod
    def format_local_secondary_index(cls, hash_key, local_secondary_index):
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
            Props.INDEX_NAME: local_secondary_index.index_name,
            Props.KEY_SCHEMA: cls.format_key_schema(
                (hash_key, local_secondary_index.attribute_to_index)
            ),
            Props.PROJECTION: projection,
            Props.INDEX_SIZE_BYTES: 0,
            Props.INDEX_SIZE_BYTES: 0
        }

    @classmethod
    def parse_local_secondary_indexes(cls, local_secondary_index_list_json):
        return map(cls.parse_local_secondary_index,
                   local_secondary_index_list_json)

    @classmethod
    def format_local_secondary_indexes(cls, hash_key,
                                       local_secondary_index_list):
        return map(lambda index: cls.format_local_secondary_index(hash_key,
                                                                  index),
                   local_secondary_index_list)

    @staticmethod
    def decode_single_value(single_value_type, encoded_single_value):
        if single_value_type == models.AttributeType.ELEMENT_TYPE_STRING:
            assert isinstance(encoded_single_value, str)
            return encoded_single_value
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            return decimal.Decimal(encoded_single_value, DECIMAL_CONTEXT)
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            return encoded_single_value.decode('base64')
        else:
            assert False, "Value type wasn't recognized"

    @staticmethod
    def encode_single_value(single_value_type, decoded_single_value):
        if single_value_type == models.AttributeType.ELEMENT_TYPE_STRING:
            assert isinstance(decoded_single_value, str)
            return decoded_single_value
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_NUMBER:
            assert isinstance(decoded_single_value, decimal.Decimal)
            return str(decoded_single_value)
        elif single_value_type == models.AttributeType.ELEMENT_TYPE_BLOB:
            return decoded_single_value.encode('base64')
        else:
            assert False, "Value type wasn't recognized"

    @classmethod
    def decode_attr_value(cls, dynamodb_attr_type, dynamodb_attr_value):
        attr_type = cls.DYNAMODB_TO_STORAGE_TYPE_MAP[dynamodb_attr_type]
        if attr_type.collection_type is not None:
            attr_value = map(
                lambda val: cls.decode_single_value(attr_type.element_type,
                                                    val),
                dynamodb_attr_value
            )
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
        expected_item_conditions = {}

        for (attr_name, dynamodb_condition) in (
                expected_attribute_conditions_json.iteritems()):
            assert len(dynamodb_condition) == 1
            (dynamodb_condition_type, dynamodb_condition_value) = (
                dynamodb_condition.items()[0]
            )
            if dynamodb_condition_type == Props.EXISTS:
                assert isinstance(dynamodb_condition_value, bool)
                expected_item_conditions[attr_name] = (
                    models.ExpectedCondition.exists()
                    if dynamodb_condition_value else
                    models.ExpectedCondition.not_exists()
                )
            elif dynamodb_condition_type == Props.VALUE:
                assert len(dynamodb_condition_value) == 1
                (dynamodb_attr_type, dynamodb_attr_value) = (
                    dynamodb_condition_value.items()[0]
                )
                expected_item_conditions[attr_name] = (
                    models.ExpectedCondition.eq(
                        cls.decode_attr_value(
                            dynamodb_attr_type, dynamodb_attr_value
                        )
                    )
                )
        return expected_item_conditions
