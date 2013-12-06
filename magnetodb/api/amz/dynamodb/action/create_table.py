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
from magnetodb.api.amz.dynamodb.action import Props
from magnetodb.api.amz.dynamodb.action import Types

from magnetodb import storage


class CreateTableDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [Props.ATTRIBUTE_DEFINITIONS, Props.KEY_SCHEMA,
                     Props.TABLE_NAME],
        "properties": {
            Props.ATTRIBUTE_DEFINITIONS: {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [Props.ATTRIBUTE_NAME,
                                 Props.ATTRIBUTE_TYPE],
                    "properties": {
                        Props.ATTRIBUTE_NAME: Types.ATTRIBUTE_NAME,
                        Props.ATTRIBUTE_TYPE: Types.ATTRIBUTE_TYPE
                    }
                }
            },
            Props.KEY_SCHEMA: Types.KEY_SCHEMA,

            Props.LOCAL_SECONDARY_INDEXES: {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [Props.INDEX_NAME, Props.KEY_SCHEMA,
                                 Props.PROJECTION],
                    "properties": {
                        Props.INDEX_NAME: Types.INDEX_NAME,
                        Props.KEY_SCHEMA: Types.KEY_SCHEMA,
                        Props.PROJECTION: {
                            "type": "object",
                            "required": [Props.NON_KEY_ATTRIBUTES,
                                         Props.PROJECTION_TYPE],
                            "properties": {
                                Props.NON_KEY_ATTRIBUTES: {
                                    "type": "array",
                                    "items": {
                                        "type": "string",
                                    }
                                },
                                Props.PROJECTION_TYPE: {
                                    "type": "string",
                                }
                            }
                        }
                    }
                }
            },

            Props.PROVISIONED_THROUGHPUT: {
                "type": "object",
                "required": [Props.READ_CAPACITY_UNITS,
                             Props.WRITE_CAPACITY_UNITS],
                "properties": {
                    Props.READ_CAPACITY_UNITS: {
                        "type": "integer"
                    },
                    Props.WRITE_CAPACITY_UNITS: {
                        "type": "integer"
                    }
                }
            },

            Props.TABLE_NAME: Types.TABLE_NAME
        }
    }

    def __call__(self):
        exclusive_start_table_name = (
            self.action_params.get(Props.EXCLUSIVE_START_TABLE_NAME, None)
        )

        limit = self.action_params.get(Props.LIMIT, None)

        table_names = (
            storage.list_tables(
                self.context,
                exclusive_start_table_name=exclusive_start_table_name,
                limit=limit)
        )

        if table_names:
            return {"LastEvaluatedTableName": table_names[-1],
                    "TableNames": table_names}
        else:
            return {}
