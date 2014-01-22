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

from magnetodb.api.amz.dynamodb.action import list_tables
from magnetodb.api.amz.dynamodb.action import create_table
from magnetodb.api.amz.dynamodb.action import describe_table
from magnetodb.api.amz.dynamodb.action import delete_table
from magnetodb.api.amz.dynamodb.action import put_item
from magnetodb.api.amz.dynamodb.action import update_item
from magnetodb.api.amz.dynamodb.action import get_item
from magnetodb.api.amz.dynamodb.action import delete_item
from magnetodb.api.amz.dynamodb.action import query
from magnetodb.api.amz.dynamodb.action import scan

capabilities = {
    'ListTables': list_tables.ListTablesDynamoDBAction,
    'DescribeTable': describe_table.DescribeTableDynamoDBAction,
    'CreateTable': create_table.CreateTableDynamoDBAction,
    'DeleteTable': delete_table.DeleteTableDynamoDBAction,
    'PutItem': put_item.PutItemDynamoDBAction,
    'UpdateItem': update_item.UpdateItemDynamoDBAction,
    'GetItem': get_item.GetItemDynamoDBAction,
    'DeleteItem': delete_item.DeleteItemDynamoDBAction,
    'Query': query.QueryDynamoDBAction,
    'Scan': scan.ScanDynamoDBAction
}
