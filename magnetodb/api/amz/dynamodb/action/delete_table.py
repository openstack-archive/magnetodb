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


class DeleteTableDynamoDBAction(action.DynamoDBAction):
    schema = {
        "required": [parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    def __call__(self):

        table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

        try:
            table_meta = storage.describe_table(self.tenant, table_name)

            storage.delete_table(self.tenant, table_name)

            # TODO(isviridov): fill ITEM_COUNT, TABLE_SIZE_BYTES,
            # CREATION_DATE_TIME with real data
            return {
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
                    parser.Props.LOCAL_SECONDARY_INDEXES: (
                        parser.Parser.format_local_secondary_indexes(
                            table_meta.schema.key_attributes[0],
                            table_meta.schema.index_def_map
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
        except Exception:
            raise amz_exception.AWSErrorResponseException()
