# Copyright 2014 Mirantis Inc.
# Copyright 2014 Symantec Corporation
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
from magnetodb.storage.table_info_repo import TableInfoRepository

import mock
import unittest

from concurrent.futures import Future

from magnetodb.storage import models
from magnetodb.storage.manager.simple_impl import SimpleStorageManager


class SimpleStorageManagerTestCase(unittest.TestCase):
    """The test for simple storage manager implementation."""

    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_key_schema')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_is_active')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_delete_item_async')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_put_item_async')
    def test_execute_write_batch(self, mock_put_item, mock_delete_item,
                                 mock_repo_get, mock_validate_table_is_active,
                                 mock_validate_key_schema):
        future = Future()
        future.set_result(True)
        mock_put_item.return_value = future
        mock_delete_item.return_value = future

        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'

        request_list = [
            models.PutItemRequest(
                table_name,
                {
                    'id': models.AttributeValue('N', 1),
                    'range': models.AttributeValue('S', '1'),
                    'str': models.AttributeValue('S', 'str1'),
                }
            ),
            models.PutItemRequest(
                table_name,
                {
                    'id': models.AttributeValue('N', 2),
                    'range': models.AttributeValue('S', '1'),
                    'str': models.AttributeValue('S', 'str1')
                }
            ),
            models.DeleteItemRequest(
                table_name,
                {
                    'id': models.AttributeValue('N', 3),
                    'range': models.AttributeValue('S', '3')
                }
            )
        ]

        expected_put = [mock.call(context, request_list[0]),
                        mock.call(context, request_list[1]), ]
        expected_delete = [mock.call(context, request_list[2])]

        storage_manager = SimpleStorageManager(None, TableInfoRepository())

        unprocessed_items = storage_manager.execute_write_batch(
            context, request_list
        )

        self.assertEqual(expected_put, mock_put_item.call_args_list)
        self.assertEqual(expected_delete,
                         mock_delete_item.call_args_list)

        self.assertEqual(unprocessed_items, [])

    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_key_schema')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_is_active')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_get_item_async')
    def test_execute_get_batch(self, mock_get_item, mock_repo_get,
                               mock_validate_table_is_active,
                               mock_validate_key_schema):
        future = Future()
        future.set_result(True)
        mock_get_item.return_value = future

        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'

        request_list = [
            models.GetItemRequest(
                table_name,
                {
                    'id': models.AttributeValue('N', 1),
                    'str': models.AttributeValue('S', 'str1'),
                },
                None,
                True
            ),
            models.GetItemRequest(
                table_name,
                {
                    'id': models.AttributeValue('N', 1),
                    'str': models.AttributeValue('S', 'str2'),
                },
                None,
                True
            )
        ]

        expected_get = [mock.call(context, req.table_name,
                                  req.key_attribute_map,
                                  req.attributes_to_get, req.consistent)
                        for req in request_list]

        storage_manager = SimpleStorageManager(None, TableInfoRepository())

        result, unprocessed_items = storage_manager.execute_get_batch(
            context, request_list
        )
        mock_get_item.has_calls(expected_get)
        self.assertEqual(unprocessed_items, [])
