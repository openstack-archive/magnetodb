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


class UpdateItemDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [parser.Props.KEY,
                     parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.EXPECTED: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "type": "object",
                        "properties": {
                            parser.Props.EXISTS: {
                                "type": "boolean",
                            },
                            parser.Props.VALUE: parser.Types.ITEM_VALUE
                        }
                    }
                }
            },

            parser.Props.ATTRIBUTE_UPDATES: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "type": "object",
                        "required": [parser.Props.ACTION],
                        "properties": {
                            parser.Props.ACTION: parser.Types.ACTION_TYPE,
                            parser.Props.VALUE: parser.Types.ITEM_VALUE
                        }
                    }
                }
            },

            parser.Props.KEY: {
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
                         parser.Values.RETURN_VALUES_ALL_OLD,
                         parser.Values.RETURN_VALUES_ALL_NEW,
                         parser.Values.RETURN_VALUES_UPDATED_OLD,
                         parser.Values.RETURN_VALUES_UPDATED_NEW]
            },
            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    def __call__(self):
        try:
            table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

            # parse expected item conditions
            expected_item_conditions = (
                parser.Parser.parse_expected_attribute_conditions(
                    self.action_params.get(parser.Props.EXPECTED, {})
                )
            )

            #parse attribute updates
            attribute_updates = parser.Parser.parse_attribute_updates(
                self.action_params.get(parser.Props.ATTRIBUTE_UPDATES, {})
            )

            # parse key
            key_attributes = parser.Parser.parse_item_attributes(
                self.action_params[parser.Props.KEY]
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

            select_result = None

            indexed_condition_map_for_select = {
                name: models.IndexedCondition.eq(value)
                for name, value in key_attributes.iteritems()
            }
        except Exception:
            raise exception.ValidationException()

        try:

            if return_values in (parser.Values.RETURN_VALUES_UPDATED_OLD,
                                 parser.Values.RETURN_VALUES_ALL_OLD):

                select_result = storage.select_item(
                    self.context, table_name,
                    indexed_condition_map_for_select)

            # update item
            result = storage.update_item(
                self.context,
                table_name,
                key_attribute_map=key_attributes,
                attribute_action_map=attribute_updates,
                expected_condition_map=expected_item_conditions)

            if not result:
                raise exception.AWSErrorResponseException()

            if return_values in (parser.Values.RETURN_VALUES_UPDATED_NEW,
                                 parser.Values.RETURN_VALUES_ALL_NEW):

                select_result = storage.select_item(
                    self.context, table_name,
                    indexed_condition_map_for_select)

            # format response
            response = {}

            if return_values != parser.Values.RETURN_VALUES_NONE:
                response[parser.Props.ATTRIBUTES] = (
                    parser.Parser.format_item_attributes(
                        select_result.items[0])
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

            if (return_consumed_capacity !=
                    parser.Values.RETURN_CONSUMED_CAPACITY_NONE):
                response[parser.Props.CONSUMED_CAPACITY] = (
                    parser.Parser.format_consumed_capacity(
                        return_consumed_capacity, None
                    )
                )

            return response
        except exception.AWSErrorResponseException as e:
            raise e
        except Exception:
            raise exception.AWSErrorResponseException()
