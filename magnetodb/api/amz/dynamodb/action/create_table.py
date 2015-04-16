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

from magnetodb.api.amz.dynamodb import action
from magnetodb.api.amz import parser

from magnetodb import storage
from magnetodb.api.amz import exception as amz_exception
from magnetodb.common import exception
from magnetodb.storage import models


class CreateTableDynamoDBAction(action.DynamoDBAction):
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

            # parse table attributes
            attribute_definitions = parser.Parser.parse_attribute_definitions(
                self.action_params.get(parser.Props.ATTRIBUTE_DEFINITIONS, {})
            )

            # parse table key schema
            key_attrs = parser.Parser.parse_key_schema(
                self.action_params.get(parser.Props.KEY_SCHEMA, [])
            )

            # parse table indexed field list
            indexed_def_map = parser.Parser.parse_local_secondary_indexes(
                self.action_params.get(
                    parser.Props.LOCAL_SECONDARY_INDEXES, [])
            )

            # prepare table_schema structure
            table_schema = models.TableSchema(
                attribute_definitions, key_attrs, indexed_def_map
            )

        except Exception:
            raise amz_exception.AWSValidationException()

        try:
            # creating table
            table_meta = storage.create_table(
                self.tenant, table_name, table_schema
            )

            result = {
                parser.Props.TABLE_DESCRIPTION: {
                    parser.Props.ATTRIBUTE_DEFINITIONS: (
                        parser.Parser.format_attribute_definitions(
                            table_meta.schema.attribute_type_map
                        )
                    ),
                    parser.Props.CREATION_DATE_TIME: 0,
                    parser.Props.ITEM_COUNT: 0,
                    parser.Props.KEY_SCHEMA: (
                        parser.Parser.format_key_schema(
                            table_meta.schema.key_attributes
                        )
                    ),
                    parser.Props.PROVISIONED_THROUGHPUT: (
                        parser.Values.PROVISIONED_THROUGHPUT_DUMMY
                    ),
                    parser.Props.TABLE_NAME: table_name,
                    parser.Props.TABLE_STATUS: (
                        parser.Parser.format_table_status(table_meta.status)
                    ),
                    parser.Props.TABLE_SIZE_BYTES: 0
                }
            }

            if table_meta.schema.index_def_map:
                table_def = result[parser.Props.TABLE_DESCRIPTION]
                table_def[parser.Props.LOCAL_SECONDARY_INDEXES] = (
                    parser.Parser.format_local_secondary_indexes(
                        table_meta.schema.key_attributes[0],
                        table_meta.schema.index_def_map
                    )
                )

            return result
        except exception.TableAlreadyExistsException:
            raise amz_exception.AWSDuplicateTableError(table_name=table_name)
        except Exception:
            raise amz_exception.AWSErrorResponseException()
