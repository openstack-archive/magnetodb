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

from magnetodb.api.amz.dynamodb.action import DynamoDBAction
from magnetodb.api.amz.dynamodb import parser

from magnetodb import storage
from magnetodb.storage import models

from magnetodb.common import exception


class PutItemDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [parser.Props.ITEM,
                     parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.EXPECTED: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "oneOf": [
                            {
                                "type": "object",
                                "required": [parser.Props.EXISTS],
                                "properties": {
                                    parser.Props.EXISTS: {
                                        "type": "boolean",
                                    },
                                }
                            },
                            {
                                "type": "object",
                                "required": [parser.Props.VALUE],
                                "properties": {
                                    parser.Props.VALUE:
                                        parser.Types.ITEM_VALUE,
                                }
                            },
                        ]
                    }
                }
            },

            parser.Props.ITEM: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },

            parser.Props.RETURN_CONSUMED_CAPACITY: (
                parser.Types.RETURN_CONSUMED_CAPACITY
            ),
            parser.Props.RETURN_ITEM_COLLECTION_METRICS: (
                parser.Types.RETURN_ITEM_COLLECTION_METRICS
            ),
            parser.Props.RETURN_VALUES: {
                "type": "string",
                "enum": [parser.Values.RETURN_VALUES_NONE,
                         parser.Values.RETURN_VALUES_ALL_OLD]
            },
            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    def __call__(self):
        table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

        # parse expected item conditions
        expected_item_conditions = (
            parser.Parser.parse_expected_item_conditions(
                self.action_params.get(parser.Props.EXPECTED, None)
            )
        )

        # parse item
        item_attributes = parser.Parser.parse_item_attributes(
            self.action_params[parser.Props.ITEM]
        )

        # parse return_values param
        return_values = self.action_params.get(
            parser.Props.RETURN_VALUES, parser.Values.RETURN_VALUES_NONE
        )

        # parse return_item_collection_metrics
        return_item_collection_metrics = self.action_params.get(
            parser.Props.RETURN_ITEM_COLLECTION_METRICS,
            parser.Values.RETURN_ITEM_COLLECTION_METRICS_NONE
        )

        return_consumed_capacity = self.action_params.get(
            parser.Props.RETURN_CONSUMED_CAPACITY,
            parser.Values.RETURN_CONSUMED_CAPACITY_NONE
        )

        # put item
        result = storage.put_item(
            self.context,
            models.PutItemRequest(table_name, item_attributes),
            if_not_exist=False,
            expected_condition_map=expected_item_conditions)

        if not result:
            raise exception.ConditionalCheckFailedException()

        # format response
        response = {}

        if return_values != parser.Values.RETURN_VALUES_NONE:
            response[parser.Props.ATTRIBUTES] = (
                parser.Parser.format_item_attributes(item_attributes)
            )

        if (return_item_collection_metrics !=
                parser.Values.RETURN_ITEM_COLLECTION_METRICS_NONE):
            response[parser.Props.ITEM_COLLECTION_METRICS] = {
                parser.Props.ITEM_COLLECTION_KEY: {
                    parser.Parser.format_item_attributes(
                        models.AttributeValue(models.ATTRIBUTE_TYPE_STRING,
                                              "key")
                    )
                },
                parser.Props.SIZE_ESTIMATED_RANGE_GB: [0]
            }

        if return_consumed_capacity in {
                parser.Values.RETURN_CONSUMED_CAPACITY_INDEXES,
                parser.Values.RETURN_CONSUMED_CAPACITY_TOTAL}:
            consumed_capacity = {
                parser.Props.GLOBAL_SECONDARY_INDEXES: {
                    # TODO(dukhlov):
                    # read schema and fill global index consumed
                    # capacity to imitate DynamoDB API
                    "global_index_name": {
                        parser.Props.CAPACITY_UNITS: 0
                    }
                },
                parser.Props.LOCAL_SECONDARY_INDEXES: {
                    # TODO(dukhlov):
                    # read schema and fill local index consumed
                    # capacity to imitate DynamoDB API
                    "local_index_name": {
                        parser.Props.CAPACITY_UNITS: 0
                    }
                }
            }

            if (return_consumed_capacity ==
                    parser.Values.RETURN_CONSUMED_CAPACITY_TOTAL):
                consumed_capacity[parser.Props.CAPACITY_UNITS] = 0
                consumed_capacity[parser.Props.TABLE] = {
                    parser.Props.CAPACITY_UNITS: 0
                }
            response[parser.Props.CONSUMED_CAPACITY] = consumed_capacity

        return response
