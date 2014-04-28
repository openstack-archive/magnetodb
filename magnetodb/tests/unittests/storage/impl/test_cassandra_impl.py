# Copyright 2014 Mirantis Inc.
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

import mock
import unittest

from magnetodb.storage import models
from magnetodb.storage.impl import cassandra_impl
from magnetodb.common import exception


class CassandraImplTestCase(unittest.TestCase):
    """The test for Cassandra storage implementation."""

    @mock.patch('magnetodb.storage.impl.cassandra_impl.'
                'CassandraStorageImpl.delete_item')
    @mock.patch('magnetodb.storage.impl.cassandra_impl.'
                'CassandraStorageImpl.put_item')
    @mock.patch('magnetodb.storage.impl.cassandra_impl.'
                'CassandraStorageImpl._execute_query')
    @mock.patch('magnetodb.common.cassandra.cluster.Cluster.connect')
    def test_execute_write_batch(self, mock_connect, mock_execute_query,
                                 mock_put_item, mock_delete_item):
        mock_execute_query.return_value = None
        mock_put_item.return_value = None
        mock_delete_item.return_value = None

        conn = cassandra_impl.CassandraStorageImpl()
        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'

        request_list = [
            models.PutItemRequest(table_name, {
                'id': models.AttributeValue(
                    models.ATTRIBUTE_TYPE_NUMBER, 1),
                'range': models.AttributeValue(
                    models.ATTRIBUTE_TYPE_STRING, '1'),
                'str': models.AttributeValue(
                    models.ATTRIBUTE_TYPE_STRING, 'str1'), }),
            models.PutItemRequest(table_name, {
                'id': models.AttributeValue(
                    models.ATTRIBUTE_TYPE_NUMBER, 2),
                'range': models.AttributeValue(
                    models.ATTRIBUTE_TYPE_STRING, '1'),
                'str': models.AttributeValue(
                    models.ATTRIBUTE_TYPE_STRING, 'str1'), }),
            models.DeleteItemRequest(table_name, {
                'id': models.AttributeValue.number(3),
                'range': models.AttributeValue.str('3')})
        ]

        expected_put = [mock.call(context, request_list[0]),
                        mock.call(context, request_list[1]), ]
        expected_delete = [mock.call(context, request_list[2])]

        unprocessed_items = conn.execute_write_batch(context, request_list)

        self.assertEqual(expected_put, mock_put_item.call_args_list)
        self.assertEqual(expected_delete,
                         mock_delete_item.call_args_list)

        self.assertEqual(unprocessed_items, [])

    @mock.patch('magnetodb.storage.impl.cassandra_impl.'
                'CassandraStorageImpl._get_table_info')
    @mock.patch('magnetodb.common.cassandra.cluster.Cluster.connect')
    def test_table_not_exist_exception_in_get_item(self,
                                                   mock_connect,
                                                   mock_table_info):
        mock_table_info.return_value = None
        conn = cassandra_impl.CassandraStorageImpl()
        context = mock.Mock(tenant='fake_tenant')

        with self.assertRaises(
                exception.TableNotExistsException) as raises_cm:
            conn.select_item(context, "nonexistenttable")

        ex = raises_cm.exception
        self.assertIn("Table 'nonexistenttable' does not exists", ex.message)

    @mock.patch('magnetodb.storage.impl.cassandra_impl.'
                'CassandraStorageImpl._get_table_info')
    @mock.patch('magnetodb.common.cassandra.cluster.Cluster.connect')
    def test_table_not_exist_exception_in_put_item(self,
                                                   mock_connect,
                                                   mock_table_info):
        mock_table_info.return_value = None
        conn = cassandra_impl.CassandraStorageImpl()
        context = mock.Mock(tenant='fake_tenant')

        with self.assertRaises(exception.TableNotExistsException) as raises_cm:
            conn.put_item(context,
                          models.PutItemRequest("nonexistenttable", {}))

        ex = raises_cm.exception
        self.assertIn("Table 'nonexistenttable' does not exists", ex.message)
