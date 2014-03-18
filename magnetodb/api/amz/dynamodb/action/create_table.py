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
                        parser.Props.PROJECTION: parser.Types.PROJECTION
                    }
                }
            },

            parser.Props.PROVISIONED_THROUGHPUT: (
                parser.Types.PROVISIONED_THROUGHPUT
            ),

            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    def __call__(self):
        try:
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
            indexed_attr_names = parser.Parser.parse_local_secondary_indexes(
                self.action_params.get(
                    parser.Props.LOCAL_SECONDARY_INDEXES, [])
            )

            #prepare table_schema structure
            table_schema = models.TableSchema(
                table_name, attribute_definitions,
                key_attrs, indexed_attr_names)

        except Exception:
            raise exception.ValidationException()

        try:
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
                            key_attrs[0], indexed_attr_names
                        )
                    ),
                    parser.Props.PROVISIONED_THROUGHPUT: (
                        parser.Values.PROVISIONED_THROUGHPUT_DUMMY
                    ),
                    parser.Props.TABLE_NAME: table_name,
                    parser.Props.TABLE_STATUS: (
                        parser.Values.TABLE_STATUS_ACTIVE
                    ),
                    parser.Props.TABLE_SIZE_BYTES: 0
                }
            }
        except exception.TableAlreadyExistsException:
            raise exception.ResourceInUseException()
        except exception.AWSErrorResponseException as e:
            raise e
        except Exception:
            raise exception.AWSErrorResponseException()
