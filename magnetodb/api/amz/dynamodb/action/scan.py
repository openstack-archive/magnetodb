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
from magnetodb.common import exception


class ScanDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.ATTRIBUTES_TO_GET: {
                "type": "array",
                "items": parser.Types.ATTRIBUTE_NAME
            },

            parser.Props.EXCLUSIVE_START_KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },

            parser.Props.LIMIT: {
                "type": "integer",
                "minimum": 0,
            },

            parser.Props.RETURN_CONSUMED_CAPACITY: (
                parser.Types.RETURN_CONSUMED_CAPACITY
            ),

            parser.Props.SCAN_FILTER: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "type": "object",
                        "properties": {
                            parser.Props.ATTRIBUTE_VALUE_LIST: {
                                "type": "array",
                                "items": parser.Types.ITEM_VALUE
                            },
                            parser.Props.COMPARISON_OPERATOR: (
                                parser.Types.SCAN_OPERATOR
                            )
                        }
                    }
                }
            },

            parser.Props.SEGMENT: {
                "type": "integer",
                "minimum": 0,
            },

            parser.Props.SELECT: parser.Types.SELECT,

            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME,

            parser.Props.TOTAL_SEGMENTS: {
                "type": "integer",
                "minimum": 1,
                "maximum": 4096,
            }
        }
    }

    def __call__(self):
        try:
            #TODO ikhudoshyn: table_name may be index name
            table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

            attrs_to_get = self.action_params.get(
                parser.Props.ATTRIBUTES_TO_GET, None)

            select = self.action_params.get(
                parser.Props.SELECT, None)

            select_type = parser.Parser.parse_select_type(select, attrs_to_get)

            limit = self.action_params.get(parser.Props.LIMIT, None)

            return_consumed_capacity = self.action_params.get(
                parser.Props.RETURN_CONSUMED_CAPACITY,
                parser.Values.RETURN_CONSUMED_CAPACITY_NONE
            )

            exclusive_start_key = self.action_params.get(
                parser.Props.EXCLUSIVE_START_KEY, None
            )

            exclusive_start_key = parser.Parser.parse_item_attributes(
                exclusive_start_key) if exclusive_start_key else None

            scan_filter = self.action_params.get(
                parser.Props.SCAN_FILTER, {}
            )

            condition_map = parser.Parser.parse_attribute_conditions(
                scan_filter
            )

            segment = self.action_params.get(parser.Props.SEGMENT, 0)
            total_segments = self.action_params.get(
                parser.Props.TOTAL_SEGMENTS, 1
            )

            assert segment < total_segments
        except Exception:
            raise exception.AWSErrorResponseException()

        try:
            result = storage.scan(
                self.context, table_name, condition_map,
                attributes_to_get=attrs_to_get, limit=limit,
                exclusive_start_key=exclusive_start_key)

            response = {
                parser.Props.COUNT: result.count,
                parser.Props.SCANNED_COUNT: result.scanned_count
            }

            if not select_type.is_count:
                response[parser.Props.ITEMS] = [
                    parser.Parser.format_item_attributes(row)
                    for row in result.items]

            if (return_consumed_capacity !=
                    parser.Values.RETURN_CONSUMED_CAPACITY_NONE):
                response[parser.Props.CONSUMED_CAPACITY] = (
                    parser.Parser.format_consumed_capacity(
                        return_consumed_capacity, None
                    )
                )

            if result.last_evaluated_key:
                response[parser.Props.LAST_EVALUATED_KEY] = (
                    parser.Parser.format_item_attributes(
                        result.last_evaluated_key
                    )
                )

            return response
        except exception.AWSErrorResponseException as e:
            raise e
        except Exception:
            raise exception.AWSErrorResponseException()
