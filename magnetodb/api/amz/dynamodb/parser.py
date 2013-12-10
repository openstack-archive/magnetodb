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

from magnetodb.storage import models


class Props():
    TABLE_NAME = "TableName"
    ATTRIBUTE_DEFINITIONS = "AttributeDefinitions"
    ATTRIBUTE_NAME = "AttributeName"
    ATTRIBUTE_TYPE = "AttributeType"
    KEY_SCHEMA = "KeySchema"
    KEY_TYPE = "KeyType"
    LOCAL_SECONDARY_INDEXES = "LocalSecondaryIndexes"
    INDEX_NAME = "IndexName"
    PROJECTION = "Projection"
    NON_KEY_ATTRIBUTES = "NonKeyAttributes"
    PROJECTION_TYPE = "ProjectionType"
    PROVISIONED_THROUGHPUT = "ProvisionedThroughput"
    READ_CAPACITY_UNITS = "ReadCapacityUnits"
    WRITE_CAPACITY_UNITS = "WriteCapacityUnits"

    TABLE_DESCRIPTION = "TableDescription"
    TABLE_SIZE_BYTES = "TableSizeBytes"
    TABLE_STATUS = "TableStatus"
    CREATION_DATE_TIME = "CreationDateTime"
    INDEX_SIZE_BYTES = "IndexSizeBytes"
    ITEM_COUNT = "ItemCount"

    TABLE_NAMES = "TableNames"
    EXCLUSIVE_START_TABLE_NAME = "ExclusiveStartTableName"
    LAST_EVALUATED_TABLE_NAME = "LastEvaluatedTableName"
    LIMIT = "Limit"


class Values():
    ATTRIBUTE_TYPE_STRING = "S"
    ATTRIBUTE_TYPE_NUMBER = "N"
    ATTRIBUTE_TYPE_BLOB = "B"
    ATTRIBUTE_TYPE_STRING_SET = "SS"
    ATTRIBUTE_TYPE_NUMBER_SET = "NS"
    ATTRIBUTE_TYPE_BLOB_SET = "BS"

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


class Types():
    ATTRIBUTE_NAME = {
        "type": "string"
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
        "type": "string"
    }

    TABLE_NAME = {
        "type": "string",
        "pattern": "^\w+",
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

        type = cls.DYNAMODB_TO_STORAGE_TYPE_MAP.get(
            dynamodb__attr_type, None
        )

        return models.AttributeDefinition(dynamodb_attr_name, type)

    @classmethod
    def format_attribute_definition(cls, attr_def):
        dynamodb_type = cls.STORAGE_TO_DYNAMODB_TYPE_MAP.get(attr_def.type,
                                                             None)

        assert (dynamodb_type, "Unknown Attribute type returned by backend: %s"
                % attr_def.type)

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
                assert (
                    hash_key_attr_name is None, "Only one HASH key is allowed"
                )

                hash_key_attr_name = dynamodb_key_attr_name
            elif dynamodb_key_type == Values.KEY_TYPE_RANGE:
                assert (
                    range_key_attr_name is None,
                    "Only one RANGE key is allowed"
                )
                range_key_attr_name = dynamodb_key_attr_name

        return (hash_key_attr_name, range_key_attr_name)

    @classmethod
    def format_key_schema(cls, key_attr_names):
        assert (
            len(key_attr_names) > 0,
            "At least HASH key should be specified. No one is given"
        )

        assert (
            len(key_attr_names) <= 2,
            "More then 2 keys given. Only one HASH and one RANGE key allowed"
        )

        res = [{
            Props.KEY_TYPE: Values.KEY_TYPE_HASH,
            Props.ATTRIBUTE_NAME: key_attr_names[0]
        }]

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

        assert (
            len(key_attrs_for_projection) > 1,
            "Range key in index wasn't specified"
        )

        return key_attrs_for_projection[1]

    @classmethod
    def format_local_secondary_index(cls, hash_key, local_secondary_index):
        return {
            Props.INDEX_NAME: local_secondary_index,
            Props.KEY_SCHEMA: cls.format_key_schema((hash_key,
                                                     local_secondary_index)),
            Props.PROJECTION: {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_ALL,
                Props.NON_KEY_ATTRIBUTES: []
            },
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
