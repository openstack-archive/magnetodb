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

from magnetodb.api.amz.dynamodb.action import DynamoDBAction
from magnetodb.api.amz.dynamodb import parser

from magnetodb import storage
from magnetodb.storage import models


class CreateTableDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [parser.Props.ATTRIBUTE_DEFINITIONS,
                     parser.Props.KEY_SCHEMA,
                     parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.ATTRIBUTE_DEFINITIONS: {
                "type": "array",
                "items": parser.Types.ATTRIBUTE_DEFINITION
            },
            parser.Props.KEY_SCHEMA: parser.Types.KEY_SCHEMA,

            parser.Props.LOCAL_SECONDARY_INDEXES: {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [parser.Props.INDEX_NAME,
                                 parser.Props.KEY_SCHEMA,
                                 parser.Props.PROJECTION],
                    "properties": {
                        parser.Props.INDEX_NAME: parser.Types.INDEX_NAME,
                        parser.Props.KEY_SCHEMA: parser.Types.KEY_SCHEMA,
                        parser.Props.PROJECTION: {
                            "type": "object",
                            "required": [parser.Props.NON_KEY_ATTRIBUTES,
                                         parser.Props.PROJECTION_TYPE],
                            "properties": {
                                parser.Props.NON_KEY_ATTRIBUTES: {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                    }
                                },
                                parser.Props.PROJECTION_TYPE: {
                                    "type": "string",
                                }
                            }
                        }
                    }
                }
            },

            parser.Props.PROVISIONED_THROUGHPUT: {
                "type": "object",
                "required": [parser.Props.READ_CAPACITY_UNITS,
                             parser.Props.WRITE_CAPACITY_UNITS],
                "properties": {
                    parser.Props.READ_CAPACITY_UNITS: {
                        "type": "integer"
                    },
                    parser.Props.WRITE_CAPACITY_UNITS: {
                        "type": "integer"
                    }
                }
            },

            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    def __call__(self):
        table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

        attribute_definitions = map(
            parser.Parser.parse_attribute_definition,
            self.action_params.get(parser.Props.ATTRIBUTE_DEFINITIONS, {})
        )
        
        key_attrs = parser.Parser.parse_key_schema(
            self.action_params.get(parser.Props.KEY_SCHEMA, [])
        )
        
        key_attrs_per_projection_list = map(
            parser.Parser.parse_key_schema,
            map(
                lambda index: index.get(parser.Props.KEY_SCHEMA, {}),
                self.action_params.get(parser.Props.LOCAL_SECONDARY_INDEXES,
                                       [])
            )
        )
        
        indexed_attr_names = []
        
        for key_attrs_for_projection in key_attrs_per_projection_list:
            assert (
                len(key_attrs_for_projection) > 1,
                "Range key in index wasn't specified"
            )
            indexed_attr_names.append(key_attrs_for_projection[1])
        
        table_schema = models.TableSchema(table_name, attribute_definitions,
                                          key_attrs, indexed_attr_names)
        
        storage.create_table(table_schema)
