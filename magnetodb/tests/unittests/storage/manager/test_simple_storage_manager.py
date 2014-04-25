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

from concurrent.futures import Future

from magnetodb.storage import models
from magnetodb.storage.manager.simple_impl import SimpleStorageManager


class SimpleStorageManagerTestCase(unittest.TestCase):
    """The test for simple storage manager implementation."""

    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                'delete_item_async')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                'put_item_async')
    def test_execute_write_batch(self, mock_put_item, mock_delete_item):
        future = Future()
        future.set_result(True)
        mock_put_item.return_value = future
        mock_delete_item.return_value = future

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

        storage_manager = SimpleStorageManager(None, None)

        unprocessed_items = storage_manager.execute_write_batch(
            context, request_list
        )

        self.assertEqual(expected_put, mock_put_item.call_args_list)
        self.assertEqual(expected_delete,
                         mock_delete_item.call_args_list)

        self.assertEqual(unprocessed_items, [])
