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
import decimal
import logging
import unittest
from boto.exception import JSONResponseError

from boto.dynamodb import types
from boto.dynamodb2 import RegionInfo
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2 import types as schema_types
from boto.dynamodb2 import fields
from magnetodb.tests.fake import magnetodb_api_fake
from boto.dynamodb2.table import Table
from magnetodb.common.exception import TableNotExistsException
from magnetodb.common.exception import TableAlreadyExistsException
from magnetodb.storage import models
from magnetodb import storage
from mox import Mox, IgnoreArg

from magnetodb.common.config import CONF

DynamoDBConnection.NumberRetries = 0


class BotoIntegrationTest(unittest.TestCase):
    """
    The integration test with boto client https://github.com/boto/boto
    """

    @classmethod
    def setUpClass(cls):
        magnetodb_api_fake.run_fake_magnetodb_api()
        cls.DYNAMODB_CON = cls.connect_boto_dynamodb()

        #enabling boto logging
        if CONF.debug:
            log = logging.getLogger('boto')
            log.setLevel(logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        magnetodb_api_fake.stop_fake_magnetodb_api()

    def setUp(self):
        self.storage_mocker = Mox()

    def tearDown(self):
        self.storage_mocker.UnsetStubs()

    @staticmethod
    def connect_boto_dynamodb(host="localhost", port=8080):
        endpoint = '{}:{}'.format(host, port)
        region = RegionInfo(name='test_server', endpoint=endpoint,
                            connection_cls=DynamoDBConnection)

        return region.connect(aws_access_key_id="asd",
                              aws_secret_access_key="asd",
                              port=port, is_secure=False,
                              validate_certs=False)

    def test_list_table(self):
        self.storage_mocker.StubOutWithMock(storage, "list_tables")
        storage.list_tables(
            IgnoreArg(), exclusive_start_table_name=None, limit=None
        ).AndReturn(['table1', 'table2'])

        self.storage_mocker.ReplayAll()
        self.assertEqual({'TableNames': ['table1', 'table2']},
                         self.DYNAMODB_CON.list_tables())

        self.storage_mocker.VerifyAll()

    def test_describe_table(self):

        self.storage_mocker.StubOutWithMock(storage, 'describe_table')

        storage.describe_table(IgnoreArg(), 'test_table').AndReturn(
            models.TableSchema(
                'test_table',
                {
                    models.AttributeDefinition(
                        'city1', models.ATTRIBUTE_TYPE_STRING),
                    models.AttributeDefinition(
                        'id', models.ATTRIBUTE_TYPE_STRING),
                    models.AttributeDefinition(
                        'name', models.ATTRIBUTE_TYPE_STRING)
                },
                ['id', 'name'],
                {models.IndexDefinition('index_name', 'city1')}
            )
        )

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        table_description = table.describe()

        self.storage_mocker.VerifyAll()

        self.assertEquals('test_table',
                          table_description['Table']['TableName'])
        self.assertItemsEqual(
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

    def test_describe_unexisting_table(self):

        self.storage_mocker.StubOutWithMock(storage, 'describe_table')

        storage.describe_table(
            IgnoreArg(), 'test_table1'
        ).AndRaise(TableNotExistsException)

        self.storage_mocker.ReplayAll()

        table = Table('test_table1', connection=self.DYNAMODB_CON)

        try:
            table.describe()
        except JSONResponseError as e:
            self.assertEqual(
                e.body['__type'],
                'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException')

            self.assertEqual(
                e.body['message'],
                'The resource which is being requested does not exist.')

    def test_delete_table(self):
        self.storage_mocker.StubOutWithMock(storage, 'delete_table')
        self.storage_mocker.StubOutWithMock(storage, 'describe_table')
        storage.delete_table(IgnoreArg(), 'test_table')

        storage.describe_table(IgnoreArg(), 'test_table').AndReturn(
            models.TableSchema(
                'test_table',
                {
                    models.AttributeDefinition(
                        'city1', models.ATTRIBUTE_TYPE_STRING),
                    models.AttributeDefinition(
                        'id', models.ATTRIBUTE_TYPE_STRING),
                    models.AttributeDefinition(
                        'name', models.ATTRIBUTE_TYPE_STRING)
                },
                ['id', 'name'],
                {models.IndexDefinition('index_name', 'city1')}
            )
        )

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        self.assertTrue(table.delete())

        self.storage_mocker.VerifyAll()

    def test_create_table(self):
        self.storage_mocker.StubOutWithMock(storage, 'create_table')
        storage.create_table(IgnoreArg(), IgnoreArg())
        self.storage_mocker.ReplayAll()

        Table.create(
            "test",
            schema=[
                fields.HashKey('hash', data_type=schema_types.NUMBER),
                fields.RangeKey('range', data_type=schema_types.STRING)
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
                                        data_type=schema_types.STRING)
                    ]
                )
            ],
            connection=self.DYNAMODB_CON
        )

        self.storage_mocker.VerifyAll()

    def test_create_table_duplicate(self):
        self.storage_mocker.StubOutWithMock(storage, 'create_table')
        storage.create_table(IgnoreArg(), IgnoreArg())
        storage.create_table(
            IgnoreArg(), IgnoreArg()
        ).AndRaise(TableAlreadyExistsException)
        self.storage_mocker.ReplayAll()

        Table.create(
            "test",
            schema=[
                fields.HashKey('hash', data_type=schema_types.NUMBER),
                fields.RangeKey('range', data_type=schema_types.STRING)
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
                                        data_type=schema_types.STRING)
                    ]
                )
            ],
            connection=self.DYNAMODB_CON
        )

        try:
            Table.create(
                "test",
                schema=[
                    fields.HashKey('hash', data_type=schema_types.NUMBER),
                    fields.RangeKey('range', data_type=schema_types.STRING)
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
                                            data_type=schema_types.STRING)
                        ]
                    )
                ],
                connection=self.DYNAMODB_CON
            )

            self.fail()
        except JSONResponseError as e:
            self.assertEqual('ResourceInUseException', e.error_code)
            self.storage_mocker.VerifyAll()
        except Exception as e:
            self.fail()

    def test_put_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'put_item')
        storage.put_item(
            IgnoreArg(), IgnoreArg(),
            if_not_exist=IgnoreArg(),
            expected_condition_map=IgnoreArg()).AndReturn(True)
        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        blob_data1 = bytes(bytearray([1, 2, 3, 4, 5]))
        blob_data2 = bytes(bytearray([5, 4, 3, 2, 1]))
        table.put_item(
            {
                "hash_key": 1,
                "range_key": "range",
                "value_blob": types.Binary(blob_data1),
                "value_blob_set": {types.Binary(blob_data1),
                                   types.Binary(blob_data2)}
            },
            False
        )

        self.storage_mocker.VerifyAll()

    def test_delete_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'delete_item')
        storage.delete_item(
            IgnoreArg(), IgnoreArg(),
            expected_condition_map=IgnoreArg()).AndReturn(True)
        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        table.delete_item(hash_key=1, range_key="range")

        self.storage_mocker.VerifyAll()

    def test_get_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'select_item')

        blob_data1 = bytes(bytearray([1, 2, 3, 4, 5]))
        blob_data2 = bytes(bytearray([5, 4, 3, 2, 1]))

        hash_key = "4.5621201231232132132132132132132142354E126"
        range_key = "range"

        storage.select_item(
            IgnoreArg(), IgnoreArg(), IgnoreArg(),
            select_type=IgnoreArg(), limit=IgnoreArg(),
            consistent=IgnoreArg()
        ).AndReturn(
            models.SelectResult(
                items=[
                    {
                        "hash_key": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_NUMBER,
                            decimal.Decimal(hash_key)
                        ),
                        "range_key": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_STRING, range_key
                        ),
                        "value_blob": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_BLOB, blob_data1
                        ),
                        "value_blob_set": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_BLOB_SET,
                            {blob_data1, blob_data2}
                        )
                    }
                ]
            )
        )

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        item = table.get_item(consistent=False, hash_key=1, range_key="range")

        expected_item = {
            "hash_key": decimal.Decimal(hash_key),
            "range_key": range_key,
            "value_blob": types.Binary(blob_data1),
            "value_blob_set": {types.Binary(blob_data1),
                               types.Binary(blob_data2)}
        }

        self.assertDictEqual(expected_item, dict(item.items()))

        self.storage_mocker.VerifyAll()

    def test_select_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'select_item')

        blob_data1 = bytes(bytearray([1, 2, 3, 4, 5]))
        blob_data2 = bytes(bytearray([5, 4, 3, 2, 1]))

        hash_key = "4.5621201231232132132132132132132142354E126"
        range_key = "range"

        storage.select_item(
            IgnoreArg(), IgnoreArg(), IgnoreArg(),
            select_type=IgnoreArg(), index_name=IgnoreArg(), limit=IgnoreArg(),
            exclusive_start_key=IgnoreArg(), consistent=IgnoreArg(),
            order_type=IgnoreArg(),
        ).AndReturn(
            models.SelectResult(
                items=[
                    {
                        "hash_key": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_NUMBER,
                            decimal.Decimal(hash_key)
                        ),
                        "range_key": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_STRING, range_key
                        ),
                        "value_blob": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_BLOB, blob_data1
                        ),
                        "value_blob_set": models.AttributeValue(
                            models.ATTRIBUTE_TYPE_BLOB_SET,
                            {blob_data1, blob_data2}
                        )
                    }
                ]
            )
        )

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        items = list(table.query(consistent=False, hash_key__eq=1))

        expected_item = {
            "hash_key": decimal.Decimal(hash_key),
            "range_key": range_key,
            "value_blob": types.Binary(blob_data1),
            "value_blob_set": {types.Binary(blob_data1),
                               types.Binary(blob_data2)}
        }

        self.assertEqual(len(items), 1)

        self.assertDictEqual(expected_item, dict(items[0].items()))

        self.storage_mocker.VerifyAll()

    def test_update_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'select_item')

        hash_key = "4.5621201231232132132132132132132142354E126"
        range_key = "range"

        storage.select_item(
            IgnoreArg(), IgnoreArg(), IgnoreArg(),
            select_type=IgnoreArg(), limit=IgnoreArg(),
            consistent=IgnoreArg()
        ).AndReturn(
            models.SelectResult(
                items=[
                    {
                        "hash_key": models.AttributeValue.number(hash_key),
                        "range_key": models.AttributeValue.str(range_key),
                        "attr_value": models.AttributeValue.str('val')
                    }
                ]
            )
        )

        self.storage_mocker.StubOutWithMock(storage, 'describe_table')

        storage.describe_table(IgnoreArg(), 'test_table').AndReturn(
            models.TableSchema(
                'test_table',
                {
                    models.AttributeDefinition(
                        'hash_key', models.ATTRIBUTE_TYPE_NUMBER),
                    models.AttributeDefinition(
                        'range_key', models.ATTRIBUTE_TYPE_STRING)
                },
                ['hash_key', 'range_key'],
                {}
            )
        )

        self.storage_mocker.StubOutWithMock(storage, 'update_item')
        storage.update_item(
            IgnoreArg(), IgnoreArg(),
            key_attribute_map=IgnoreArg(),
            attribute_action_map=IgnoreArg(),
            expected_condition_map=IgnoreArg()).AndReturn(True)

        self.storage_mocker.ReplayAll()

        table = Table('test_table', connection=self.DYNAMODB_CON)

        item = table.get_item(consistent=False, hash_key=1, range_key="range")

        item['attr_value'] = 'updated'

        item.partial_save()

        self.storage_mocker.VerifyAll()
