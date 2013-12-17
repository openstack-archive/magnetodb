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
                # "maximum": TOTAL_SEGMENT - 1
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
        raise NotImplementedError
