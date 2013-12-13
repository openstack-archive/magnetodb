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


class PutItemDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [parser.Props.ATTRIBUTE_DEFINITIONS,
                     parser.Props.KEY_SCHEMA,
                     parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.EXPECTED: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "oneOf" [
                            parser.Props.EXISTS: {
                                "type": "boolean",
                            },
                            parser.Props.VALUE: parser.Types.ITEM_VALUE
                        ]
                    }
                }
            },
            parser.Props.ITEM: parser.Types.ITEM_VALUE,

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

    #TODO: change body to correct one
    def __call__(self):
        table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

        #parse table attributes
        attribute_definitions = parser.Parser.parse_attribute_definitions(
            self.action_params.get(parser.Props.ATTRIBUTE_DEFINITIONS, {})
        )

        #parse table key schema
        key_attrs = parser.Parser.parse_key_schema(
            self.action_params.get(parser.Props.KEY_SCHEMA, [])
        )

        #parse table indexed field list
        index_defs = parser.Parser.parse_local_secondary_indexes(
            self.action_params.get(parser.Props.LOCAL_SECONDARY_INDEXES, [])
        )

        #prepare table_schema structure
        table_schema = models.TableSchema(table_name, attribute_definitions,
                                          key_attrs, index_defs)

        # creating table
        storage.create_table(self.context, table_schema)

        return {
            parser.Props.TABLE_DESCRIPTION: {
                parser.Props.ATTRIBUTE_DEFINITIONS: (
                    parser.Parser.format_attribute_definitions(
                        attribute_definitions
                    )
                ),
                parser.Props.CREATION_DATE_TIME: 0,
                parser.Props.ITEM_COUNT: 0,
                parser.Props.KEY_SCHEMA: (
                    parser.Parser.format_key_schema(
                        key_attrs
                    )
                ),
                parser.Props.LOCAL_SECONDARY_INDEXES: (
                    parser.Parser.format_local_secondary_indexes(
                        key_attrs[0], index_defs
                    )
                ),
                parser.Props.PROVISIONED_THROUGHPUT: (
                    parser.Values.PROVISIONED_THROUGHPUT_DUMMY
                ),
                parser.Props.TABLE_NAME: table_name,
                parser.Props.TABLE_STATUS: parser.Values.TABLE_STATUS_ACTIVE,
                parser.Props.TABLE_SIZE_BYTES: 0
            }
        }
