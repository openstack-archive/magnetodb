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

from boto.dynamodb2 import RegionInfo
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2 import types
from boto.dynamodb2 import fields
import os
import unittest

from magnetodb.tests.fake import magnetodb_api_fake
from magnetodb.common import PROJECT_ROOT_DIR
from boto.dynamodb2.table import Table
from magnetodb.storage.models import AttributeDefinition,\
    ATTRIBUTE_TYPE_STRING, TableSchema
import magnetodb.storage as Storage

from mox import Mox, IgnoreArg

CONF = magnetodb_api_fake.CONF


class BotoIntegrationTest(unittest.TestCase):
    CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR,
                               'etc/magnetodb-test.conf')

    PASTE_CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR,
                                     'etc/api-paste-test.ini')

    @classmethod
    def setUpClass(cls):
        magnetodb_api_fake.run_fake_magnetodb_api(cls.PASTE_CONFIG_FILE)
        cls.DYNAMODB_CON = cls.connect_boto_dynamodb()

        cls.STORAGE = Storage

    @classmethod
    def tearDownClass(cls):
        magnetodb_api_fake.stop_fake_magnetodb_api()

    @staticmethod
    def connect_boto_dynamodb(host=None, port=None):
        if not host:
            host = CONF.bind_host

        if not port:
            port = CONF.bind_port

        endpoint = '{}:{}'.format(host, port)
        region = RegionInfo(name='test_server', endpoint=endpoint,
                            connection_cls=DynamoDBConnection)

        return region.connect(aws_access_key_id="asd",
                              aws_secret_access_key="asd",
                              port=port, is_secure=False,
                              validate_certs=False)

    def setUp(self):
        self.storage_mocker = Mox()

    def tearDown(self):
        self.storage_mocker.UnsetStubs()

    def testListTable(self):
        self.storage_mocker.StubOutWithMock(self.STORAGE, "list_tables")
        self.STORAGE.list_tables(IgnoreArg(),
                                 exclusive_start_table_name=None, limit=None)\
            .AndReturn(['table1', 'table2'])

        self.storage_mocker.ReplayAll()
        self.assertEqual({'TableNames': ['table1', 'table2']},
                         self.DYNAMODB_CON.list_tables())

        self.storage_mocker.VerifyAll()

    def testDescribeTable(self):

        self.storage_mocker.StubOutWithMock(self.STORAGE, 'describe_table')

        self.STORAGE.describe_table(IgnoreArg(), 'test_table').\
            AndReturn(TableSchema('test_table',
                                  [AttributeDefinition('city1',
                                                       ATTRIBUTE_TYPE_STRING),
                                   AttributeDefinition('id',
                                                       ATTRIBUTE_TYPE_STRING),
                                   AttributeDefinition('name',
                                                       ATTRIBUTE_TYPE_STRING)],
                                  ['id', 'name'], ['city1']))

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        table_description = table.describe()

        self.storage_mocker.VerifyAll()

        self.assertEquals('test_table',
                          table_description['Table']['TableName'])
        self.assertListEqual(
            [
                {
                    "AttributeName": "city1",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "id",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "name",
                    "AttributeType": "S"
                }
            ], table_description['Table']['AttributeDefinitions'])

    def testDeleteTable(self):
        self.storage_mocker.StubOutWithMock(self.STORAGE, 'delete_table')
        self.storage_mocker.StubOutWithMock(self.STORAGE, 'describe_table')
        self.STORAGE.delete_table(IgnoreArg(), 'test_table')

        self.STORAGE.describe_table(IgnoreArg(), 'test_table').\
            AndReturn(TableSchema('test_table',
                                  [AttributeDefinition('city1',
                                                       ATTRIBUTE_TYPE_STRING),
                                   AttributeDefinition('id',
                                                       ATTRIBUTE_TYPE_STRING),
                                   AttributeDefinition('name',
                                                       ATTRIBUTE_TYPE_STRING)],
                                  ['id', 'name'], ['city1']))

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        self.assertTrue(table.delete())

        self.storage_mocker.VerifyAll()

    def testCreateTable(self):
        self.storage_mocker.StubOutWithMock(self.STORAGE, 'create_table')
        self.STORAGE.create_table(IgnoreArg(), IgnoreArg())
        self.storage_mocker.ReplayAll()

        Table.create(
            "test",
            schema=[
                fields.HashKey('hash', data_type=types.NUMBER),
                fields.RangeKey('range', data_type=types.STRING)
            ],
            throughput={
                'read': 20,
                'write': 10,
            },
            indexes=[
                fields.KeysOnlyIndex(
                    'index_name',
                    parts=[
                        fields.RangeKey('indexed_field',
                                        data_type=types.STRING)
                    ]
                )
            ],
            connection=self.DYNAMODB_CON
        )
