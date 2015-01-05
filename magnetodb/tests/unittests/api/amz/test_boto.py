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
import mox
import unittest

from boto import exception as boto_exc
from boto import dynamodb2
from boto.dynamodb import types
from boto.dynamodb2 import layer1
from boto.dynamodb2 import types as schema_types
from boto.dynamodb2 import fields
from boto.dynamodb2 import table as ddb_table
from magnetodb.common import exception
from magnetodb.storage import models
from magnetodb import storage
from magnetodb.tests.fake import magnetodb_api_fake

from magnetodb.common import config

CONF = config.CONF
layer1.DynamoDBConnection.NumberRetries = 0


class BotoIntegrationTest(unittest.TestCase):
    """
    The integration test with boto client https://github.com/boto/boto
    """

    @classmethod
    def setUpClass(cls):
        magnetodb_api_fake.run_fake_magnetodb_api()
        cls.DYNAMODB_CON = cls.connect_boto_dynamodb()

        # enabling boto logging
        if CONF.debug:
            log = logging.getLogger('boto')
            log.setLevel(logging.DEBUG)

    @classmethod
    def tearDownClass(cls):
        magnetodb_api_fake.stop_fake_magnetodb_api()

    def setUp(self):
        self.storage_mocker = mox.Mox()

    def tearDown(self):
        self.storage_mocker.UnsetStubs()

    @staticmethod
    def connect_boto_dynamodb(host="localhost", port=8080):
        endpoint = '{}:{}'.format(host, port)
        region = dynamodb2.RegionInfo(
            name='test_server', endpoint=endpoint,
            connection_cls=layer1.DynamoDBConnection
        )

        return region.connect(aws_access_key_id="asd",
                              aws_secret_access_key="asd",
                              port=port, is_secure=False,
                              validate_certs=False)

    def test_list_table(self):
        self.storage_mocker.StubOutWithMock(storage, "list_tables")
        storage.list_tables(
            mox.IgnoreArg(), exclusive_start_table_name=None, limit=None
        ).AndReturn(['table1', 'table2'])

        self.storage_mocker.ReplayAll()
        self.assertEqual({'TableNames': ['table1', 'table2']},
                         self.DYNAMODB_CON.list_tables())

        self.storage_mocker.VerifyAll()

    def test_describe_table(self):

        self.storage_mocker.StubOutWithMock(storage, 'describe_table')

        storage.describe_table(mox.IgnoreArg(), 'test_table').AndReturn(
            models.TableMeta(
                '00000000-0000-0000-0000-000000000000',
                models.TableSchema(
                    {
                        'city1': models.AttributeType('S'),
                        'id': models.AttributeType('S'),
                        'name': models.AttributeType('S')
                    },
                    ['id', 'name'],
                    {'index_name': models.IndexDefinition('id', 'city1')}
                ),
                models.TableMeta.TABLE_STATUS_ACTIVE,
                None
            )
        )

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        table_description = table.describe()

        self.storage_mocker.VerifyAll()

        self.assertEqual('test_table',
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
            mox.IgnoreArg(), 'test_table1'
        ).AndRaise(exception.TableNotExistsException)

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table1',
            connection=self.DYNAMODB_CON
        )

        try:
            table.describe()
        except boto_exc.JSONResponseError as e:
            self.assertEqual(
                e.body['__type'],
                'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException')

            self.assertEqual(
                e.body['message'],
                'The resource which is being requested does not exist.')

    def test_delete_table(self):
        self.storage_mocker.StubOutWithMock(storage, 'delete_table')
        self.storage_mocker.StubOutWithMock(storage, 'describe_table')
        storage.delete_table(mox.IgnoreArg(), 'test_table')

        storage.describe_table(mox.IgnoreArg(), 'test_table').AndReturn(
            models.TableMeta(
                '00000000-0000-0000-0000-000000000000',
                models.TableSchema(
                    {
                        'city1': models.AttributeType('S'),
                        'id': models.AttributeType('S'),
                        'name': models.AttributeType('S')
                    },
                    ['id', 'name'],
                    {'index_name': models.IndexDefinition('id', 'city1')}
                ),
                models.TableMeta.TABLE_STATUS_ACTIVE,
                None
            )
        )

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        self.assertTrue(table.delete())

        self.storage_mocker.VerifyAll()

    def test_create_table(self):
        self.storage_mocker.StubOutWithMock(storage, 'create_table')
        storage.create_table(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg()
        ).AndReturn(
            models.TableMeta(
                '00000000-0000-0000-0000-000000000000',
                models.TableSchema(
                    {
                        'hash': models.AttributeType('N'),
                        'range': models.AttributeType('S'),
                        'indexed_field': models.AttributeType('S')
                    },
                    ['hash', 'range'],
                    {
                        "index_name": models.IndexDefinition('hash',
                                                             'indexed_field')
                    }
                ),
                models.TableMeta.TABLE_STATUS_ACTIVE,
                None
            )
        )
        self.storage_mocker.ReplayAll()

        ddb_table.Table.create(
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
                        fields.HashKey('hash', data_type=schema_types.NUMBER),
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
        storage.create_table(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg()
        ).AndReturn(
            models.TableMeta(
                '00000000-0000-0000-0000-000000000000',
                models.TableSchema(
                    {
                        'hash': models.AttributeType('N'),
                        'range': models.AttributeType('S'),
                        'indexed_field': models.AttributeType('S')
                    },
                    ['hash', 'range'],
                    {
                        "index_name": models.IndexDefinition('hash',
                                                             'indexed_field')
                    }
                ),
                models.TableMeta.TABLE_STATUS_ACTIVE,
                None
            )
        )
        storage.create_table(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg()
        ).AndRaise(exception.TableAlreadyExistsException)
        self.storage_mocker.ReplayAll()

        ddb_table.Table.create(
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
                        fields.HashKey('hash', data_type=schema_types.NUMBER),
                        fields.RangeKey('indexed_field',
                                        data_type=schema_types.STRING)
                    ]
                )
            ],
            connection=self.DYNAMODB_CON
        )

        try:
            ddb_table.Table.create(
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
                            fields.HashKey('hash',
                                           data_type=schema_types.NUMBER),
                            fields.RangeKey('indexed_field',
                                            data_type=schema_types.STRING)
                        ]
                    )
                ],
                connection=self.DYNAMODB_CON
            )

            self.fail()
        except boto_exc.JSONResponseError as e:
            self.assertEqual('ResourceInUseException', e.error_code)
            self.assertEqual('Table already exists: test', e.body['message'])
            self.storage_mocker.VerifyAll()
        except Exception as e:
            self.fail()

    def test_create_table_no_range(self):
        self.storage_mocker.StubOutWithMock(storage, 'create_table')
        storage.create_table(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg()
        ).AndReturn(
            models.TableMeta(
                '00000000-0000-0000-0000-000000000000',
                models.TableSchema(
                    {
                        'hash': models.AttributeType('N'),
                        'indexed_field': models.AttributeType('S')
                    },
                    ['hash'],
                    {
                        "index_name": models.IndexDefinition('hash',
                                                             'indexed_field')
                    }
                ),
                models.TableMeta.TABLE_STATUS_ACTIVE,
                None
            )
        )
        self.storage_mocker.ReplayAll()

        ddb_table.Table.create(
            "test",
            schema=[
                fields.HashKey('hash', data_type=schema_types.NUMBER),
            ],
            throughput={
                'read': 20,
                'write': 10,
            },
            indexes=[
                fields.KeysOnlyIndex(
                    'index_name',
                    parts=[
                        fields.HashKey('hash', data_type=schema_types.NUMBER),
                        fields.RangeKey('indexed_field',
                                        data_type=schema_types.STRING)
                    ]
                )
            ],
            connection=self.DYNAMODB_CON
        )

        self.storage_mocker.VerifyAll()

    def test_put_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'put_item')
        storage.put_item(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
            if_not_exist=mox.IgnoreArg(),
            expected_condition_map=mox.IgnoreArg()).AndReturn((True, None))
        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        blob_data1 = bytes(bytearray([1, 2, 3, 4, 5]))
        blob_data2 = bytes(bytearray([5, 4, 3, 2, 1]))
        table.put_item(
            {
                "hash_key": 1,
                "range_key": "range",
                "value_blob": types.Binary(blob_data1),
                "value_blob_set": set([types.Binary(blob_data1),
                                       types.Binary(blob_data2)])
            },
            False
        )

        self.storage_mocker.VerifyAll()

    def test_delete_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'delete_item')
        storage.delete_item(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
            expected_condition_map=mox.IgnoreArg()
        ).AndReturn(True)
        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        table.delete_item(hash_key=1, range_key="range")

        self.storage_mocker.VerifyAll()

    def test_get_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'get_item')

        blob_data1 = bytes(bytearray([1, 2, 3, 4, 5]))
        blob_data2 = bytes(bytearray([5, 4, 3, 2, 1]))

        hash_key = "4.5621201231232132132132132132132142354E126"
        range_key = "range"

        storage.get_item(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
            select_type=mox.IgnoreArg(),
            consistent=mox.IgnoreArg()
        ).AndReturn(
            models.SelectResult(
                items=[
                    {
                        "hash_key": models.AttributeValue('N', hash_key),
                        "range_key": models.AttributeValue('S', range_key),
                        "value_blob": models.AttributeValue(
                            'B', decoded_value=blob_data1
                        ),
                        "value_blob_set": models.AttributeValue(
                            'BS', decoded_value={blob_data1, blob_data2}
                        )
                    }
                ]
            )
        )

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        item = table.get_item(consistent=False, hash_key=1, range_key="range")

        expected_item = {
            "hash_key": decimal.Decimal(hash_key),
            "range_key": range_key,
            "value_blob": types.Binary(blob_data1),
            "value_blob_set": set([types.Binary(blob_data1),
                                   types.Binary(blob_data2)])
        }

        self.assertDictEqual(expected_item, dict(item.items()))

        self.storage_mocker.VerifyAll()

    def test_query(self):
        self.storage_mocker.StubOutWithMock(storage, 'query')

        blob_data1 = bytes(bytearray([1, 2, 3, 4, 5]))
        blob_data2 = bytes(bytearray([5, 4, 3, 2, 1]))

        hash_key = "4.5621201231232132132132132132132142354E126"
        range_key = "range"

        storage.query(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
            select_type=mox.IgnoreArg(), index_name=mox.IgnoreArg(),
            limit=mox.IgnoreArg(), exclusive_start_key=mox.IgnoreArg(),
            consistent=mox.IgnoreArg(), order_type=mox.IgnoreArg(),
        ).AndReturn(
            models.SelectResult(
                items=[
                    {
                        "hash_key": models.AttributeValue('N', hash_key),
                        "range_key": models.AttributeValue('S', range_key),
                        "value_blob": models.AttributeValue(
                            'B', decoded_value=blob_data1
                        ),
                        "value_blob_set": models.AttributeValue(
                            'BS', decoded_value={blob_data1, blob_data2}
                        )
                    }
                ]
            )
        )

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        items = list(table.query(consistent=False, hash_key__eq=1))

        expected_item = {
            "hash_key": decimal.Decimal(hash_key),
            "range_key": range_key,
            "value_blob": types.Binary(blob_data1),
            "value_blob_set": set([types.Binary(blob_data1),
                                   types.Binary(blob_data2)])
        }

        self.assertEqual(len(items), 1)

        self.assertDictEqual(expected_item, dict(items[0].items()))

        self.storage_mocker.VerifyAll()

    def test_select_item_count(self):
        self.storage_mocker.StubOutWithMock(storage, 'query')

        storage.query(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
            select_type=models.SelectType.count(), index_name=mox.IgnoreArg(),
            limit=mox.IgnoreArg(), exclusive_start_key=mox.IgnoreArg(),
            consistent=mox.IgnoreArg(), order_type=mox.IgnoreArg()
        ).AndReturn(
            models.SelectResult(
                count=100500
            )
        )

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        count = table.query_count(consistent=False, hash_key__eq=1)

        self.assertEqual(count, 100500)

        self.storage_mocker.VerifyAll()

    def test_update_item(self):
        self.storage_mocker.StubOutWithMock(storage, 'get_item')

        hash_key = "4.5621201231232132132132132132132142354E126"
        range_key = "range"

        storage.get_item(
            mox.IgnoreArg(), mox.IgnoreArg(), mox.IgnoreArg(),
            select_type=mox.IgnoreArg(),
            consistent=mox.IgnoreArg()
        ).AndReturn(
            models.SelectResult(
                items=[
                    {
                        "hash_key": models.AttributeValue('N', hash_key),
                        "range_key": models.AttributeValue('S', range_key),
                        "attr_value": models.AttributeValue('S', 'val')
                    }
                ]
            )
        )

        self.storage_mocker.StubOutWithMock(storage, 'describe_table')

        storage.describe_table(mox.IgnoreArg(), 'test_table').AndReturn(
            models.TableMeta(
                '00000000-0000-0000-0000-000000000000',
                models.TableSchema(
                    {
                        'hash_key': models.AttributeType('N'),
                        'range_key': models.AttributeType('S')
                    },
                    ['hash_key', 'range_key'],
                ),
                models.TableMeta.TABLE_STATUS_ACTIVE,
                None
            )
        )

        self.storage_mocker.StubOutWithMock(storage, 'update_item')
        storage.update_item(
            mox.IgnoreArg(), mox.IgnoreArg(),
            key_attribute_map=mox.IgnoreArg(),
            attribute_action_map=mox.IgnoreArg(),
            expected_condition_map=mox.IgnoreArg()).AndReturn((True, None))

        self.storage_mocker.ReplayAll()

        table = ddb_table.Table(
            'test_table',
            connection=self.DYNAMODB_CON
        )

        item = table.get_item(consistent=False, hash_key=1, range_key="range")

        item['attr_value'] = 'updated'

        item.partial_save()

        self.storage_mocker.VerifyAll()
