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
    EXCLUSIVE_START_TABLE_NAME = "ExclusiveStartTableName"
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


class Types():
    ATTRIBUTE_NAME = {
        "type": "string"
    }

    ATTRIBUTE_TYPE = {
        "type": "string",
        "oneOf": [Values.ATTRIBUTE_TYPE_STRING,
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
        "oneOf": [Values.KEY_TYPE_HASH, Values.KEY_TYPE_RANGE]
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
        dynamodb_type = cls.STORAGE_TO_DYNAMODB_TYPE_MAP.get(attr_def.value,
                                                             None)

        assert (dynamodb_type, "Unknown Attribute type returned by backend: %s"
                % attr_def.value)

        return {
            Props.ATTRIBUTE_NAME: attr_def.name,
            Props.ATTRIBUTE_TYPE: dynamodb_type
        }

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
