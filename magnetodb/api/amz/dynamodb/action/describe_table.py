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


from magnetodb import storage
from magnetodb.api.amz.dynamodb.action import DynamoDBAction
from magnetodb.api.amz.dynamodb.parser import Props, Parser, Types, Values
from magnetodb.common import exception


class DescribeTableDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [Props.TABLE_NAME],
        "properties": {
            Props.TABLE_NAME: Types.TABLE_NAME
        }
    }

    def __call__(self):

        table_name = self.action_params.get(Props.TABLE_NAME, None)

        if not table_name:
            raise exception.ValidationException(
                message='Table name is not defined')

        try:
            table_schema = storage.describe_table(self.context, table_name)

            return {
                Props.TABLE: {
                    Props.ATTRIBUTE_DEFINITIONS: (
                        Parser.format_attribute_definitions(
                            table_schema.attribute_type_map
                        )
                    ),
                    Props.CREATION_DATE_TIME: 0,
                    Props.ITEM_COUNT: 0,
                    Props.KEY_SCHEMA: (
                        Parser.format_key_schema(
                            table_schema.key_attributes
                        )
                    ),
                    Props.LOCAL_SECONDARY_INDEXES: (
                        Parser.format_local_secondary_indexes(
                            table_schema.key_attributes[0],
                            table_schema.index_def_map
                        )
                    ),
                    Props.PROVISIONED_THROUGHPUT: (
                        Values.PROVISIONED_THROUGHPUT_DUMMY
                    ),
                    Props.TABLE_NAME: table_schema.table_name,
                    Props.TABLE_STATUS: Values.TABLE_STATUS_ACTIVE,
                    Props.TABLE_SIZE_BYTES: 0
                }
            }

        except exception.TableNotExistsException as e:
            raise exception.ResourceNotFoundException(e.message)
        except exception.AWSErrorResponseException as e:
            raise e
        except Exception:
            raise exception.AWSErrorResponseException()
