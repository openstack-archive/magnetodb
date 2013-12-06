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

from datetime import datetime

from magnetodb.api.amz.dynamodb.action import DynamoDBAction
from magnetodb.api.amz.dynamodb.action import Props
from magnetodb.api.amz.dynamodb.action import Types

from magnetodb import storage

class DeleteTableDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [Props.TABLE_NAME],
        "properties": {
            Props.TABLE_NAME: Types.TABLE_NAME
        }
    }

    def __call__(self):

        table_name = self.action_params.get(Props.TABLE_NAME, None)

        if table_name:
            storage.delete_table(self.context, table_name)
        '''
                return {"AttributeDefinitions":[],
                        "CreationDateTime":datetime.utcnow(),
                        "ItemCount":10,
                        "KeySchema":[
                                    {"AttributeName":"keyname","KeyType":"HASH"},
                                    {"AttributeName":"rangekeyname","KeyType":"RANGE"},
                                ]
                        },
                        "LocalSecondaryIndexes":[
                                                 {"IndexName":"testindex",
                                                  "IndexSizeBytes":100,
                                                  "ItemCount":200,
                                                  "KeySchema":[
                                                               {"AttributeName":"keyname","KeyType":"HASH"},
                                                               {"AttributeName":"rangekeyname","KeyType":"RANGE"},
                                                            ],
                                                  }
                                                 ]
        '''
        return {}