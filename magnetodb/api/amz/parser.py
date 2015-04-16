# Copyright 2014 Symantec Corporation
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

from oslo_serialization import jsonutils as json

from magnetodb.api.amz import exception
from magnetodb.storage import models


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

    ATTRIBUTES_TO_GET = "AttributesToGet"
    CONSISTENT_READ = "ConsistentRead"
    KEY = "Key"

    EXCLUSIVE_START_KEY = "ExclusiveStartKey"
    SCAN_FILTER = "ScanFilter"
    SELECT = "Select"
    SEGMENT = "Segment"
    TOTAL_SEGMENTS = "TotalSegments"
    ATTRIBUTE_VALUE_LIST = "AttributeValueList"
    COMPARISON_OPERATOR = "ComparisonOperator"

    EXCLUSIVE_START_KEY = "ExclusiveStartKey"
    KEY_CONDITIONS = "KeyConditions"
    SCAN_INDEX_FORWARD = "ScanIndexForward"
    SELECT = "Select"

    COUNT = "Count"
    SCANNED_COUNT = "ScannedCount"
    ITEMS = "Items"
    LAST_EVALUATED_KEY = "LastEvaluatedKey"

    ATTRIBUTE_UPDATES = "AttributeUpdates"
    ACTION = "Action"


class Values():
    KEY_TYPE_HASH = "HASH"
    KEY_TYPE_RANGE = "RANGE"

    PROJECTION_TYPE_KEYS_ONLY = "KEYS_ONLY"
    PROJECTION_TYPE_INCLUDE = "INCLUDE"
    PROJECTION_TYPE_ALL = "ALL"

    ACTION_TYPE_PUT = models.UpdateItemAction.UPDATE_ACTION_PUT
    ACTION_TYPE_ADD = models.UpdateItemAction.UPDATE_ACTION_ADD
    ACTION_TYPE_DELETE = models.UpdateItemAction.UPDATE_ACTION_DELETE

    PROVISIONED_THROUGHPUT_DUMMY = {
        "LastDecreaseDateTime": 0,
        "LastIncreaseDateTime": 0,
        "NumberOfDecreasesToday": 0,
        "ReadCapacityUnits": 0,
        "WriteCapacityUnits": 0
    }

    TABLE_STATUS_ACTIVE = models.TableMeta.TABLE_STATUS_ACTIVE
    TABLE_STATUS_CREATING = models.TableMeta.TABLE_STATUS_CREATING
    TABLE_STATUS_DELETING = models.TableMeta.TABLE_STATUS_DELETING
    TABLE_STATUS_UPDATING = "UPDATING"

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

    ALL_ATTRIBUTES = models.SelectType.SELECT_TYPE_ALL
    ALL_PROJECTED_ATTRIBUTES = models.SelectType.SELECT_TYPE_ALL_PROJECTED
    SPECIFIC_ATTRIBUTES = models.SelectType.SELECT_TYPE_SPECIFIC
    COUNT = models.SelectType.SELECT_TYPE_COUNT

    EQ = models.Condition.CONDITION_TYPE_EQUAL

    LE = models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL
    LT = models.IndexedCondition.CONDITION_TYPE_LESS
    GE = models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL
    GT = models.IndexedCondition.CONDITION_TYPE_GREATER
    BEGINS_WITH = "BEGINS_WITH"
    BETWEEN = "BETWEEN"

    NE = models.ScanCondition.CONDITION_TYPE_NOT_EQUAL
    CONTAINS = models.ScanCondition.CONDITION_TYPE_CONTAINS
    NOT_CONTAINS = models.ScanCondition.CONDITION_TYPE_NOT_CONTAINS
    IN = models.ScanCondition.CONDITION_TYPE_IN

    NOT_NULL = "NOT_NULL"
    NULL = "NULL"


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
    @classmethod
    def parse_attribute_definition(cls, attr_def_json):
        dynamodb_attr_name = attr_def_json.get(Props.ATTRIBUTE_NAME, None)
        dynamodb_attr_type = attr_def_json.get(Props.ATTRIBUTE_TYPE, "")

        storage_type = models.AttributeType(dynamodb_attr_type)

        return dynamodb_attr_name, storage_type

    @classmethod
    def format_attribute_definition(cls, attr_name, attr_type):
        dynamodb_type = attr_type.type

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
                if hash_key_attr_name is not None:
                    raise exception.AWSValidationException(
                        "Only one 'HASH' key is allowed"
                    )
                hash_key_attr_name = dynamodb_key_attr_name
            elif dynamodb_key_type == Values.KEY_TYPE_RANGE:
                if range_key_attr_name is not None:
                    raise exception.AWSValidationException(
                        "Only one 'RANGE' key is allowed"
                    )
                range_key_attr_name = dynamodb_key_attr_name
            else:
                raise exception.AWSValidationException(
                    "Only 'RANGE' or 'HASH' key types are allowed, but '{}' "
                    "is found".format(dynamodb_key_type))
        if hash_key_attr_name is None:
            raise exception.AWSValidationException("HASH key is missing")
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
        hash_key = key_attrs_for_projection[0]

        try:
            range_key = key_attrs_for_projection[1]
        except IndexError:
            raise exception.AWSValidationException(
                "Range key in index wasn't specified"
            )

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

        return index_name, models.IndexDefinition(hash_key,
                                                  range_key,
                                                  projected_attrs)

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
                (hash_key, local_secondary_index.alt_range_key_attr)
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
    def decode_attr_value(cls, dynamodb_attr_type, dynamodb_attr_value):
        return models.AttributeValue(dynamodb_attr_type, dynamodb_attr_value)

    @classmethod
    def encode_attr_value(cls, attr_value):
        return {
            attr_value.attr_type.type: attr_value.encoded_value
        }

    @classmethod
    def parse_typed_attr_value(cls, typed_attr_value_json):
        if len(typed_attr_value_json) != 1:
            raise exception.AWSValidationException(
                "Can't recognize attribute format ['{}']".format(
                    json.dumps(typed_attr_value_json)
                )
            )
        (attr_type_json, attr_value_json) = typed_attr_value_json.items()[0]

        return models.AttributeValue(attr_type_json, attr_value_json)

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
                    models.ExpectedCondition.eq(
                        cls.parse_typed_attr_value(condition_json[Props.VALUE])
                    )
                ]

            elif Props.EXISTS in condition_json:
                condition_value_json = condition_json[Props.EXISTS]

                assert isinstance(condition_value_json, bool)

                expected_attribute_conditions[attr_name_json] = [
                    models.ExpectedCondition.not_null()
                    if condition_value_json else
                    models.ExpectedCondition.null()
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
                return models.SelectType.specific_attributes(
                    attributes_to_get
                )
            else:
                if select_on_index:
                    return models.SelectType.all_projected()
                else:
                    return models.SelectType.all()

        if select == Values.SPECIFIC_ATTRIBUTES:
            assert attributes_to_get
            return models.SelectType.specific_attributes(attributes_to_get)

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
    def parse_attribute_condition(cls, condition_type, condition_args,
                                  condition_class=models.IndexedCondition):

        actual_args_count = (
            len(condition_args) if condition_args is not None else 0
        )
        if condition_type == Values.BETWEEN:
            if actual_args_count != 2:
                raise exception.AWSValidationException(
                    "{} condition type requires exactly 2 arguments, "
                    "but {} given".format(condition_type, actual_args_count),
                )
            if condition_args[0].attr_type != condition_args[1].attr_type:
                raise exception.AWSValidationException(
                    "{} condition type requires arguments of the "
                    "same type, but different types given".format(
                        condition_type
                    ),
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
                models.AttributeValue(
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
                                   condition_class=models.IndexedCondition):
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
    def format_table_status(cls, table_status):
        if table_status == models.TableMeta.TABLE_STATUS_ACTIVE:
            return Values.TABLE_STATUS_ACTIVE
        elif table_status in (models.TableMeta.TABLE_STATUS_CREATING,
                              models.TableMeta.TABLE_STATUS_CREATE_FAILED):
            return Values.TABLE_STATUS_CREATING
        elif table_status in (models.TableMeta.TABLE_STATUS_DELETING,
                              models.TableMeta.TABLE_STATUS_DELETE_FAILED):
            return Values.TABLE_STATUS_DELETING
        else:
            assert False, (
                "Table status '{}' is not allowed".format(table_status)
            )
