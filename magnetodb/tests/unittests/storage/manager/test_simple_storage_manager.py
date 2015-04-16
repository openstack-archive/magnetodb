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

import datetime
from concurrent import futures
import mock
import unittest

from oslo_utils import timeutils

from magnetodb import context as req_context
from magnetodb.common import exception
from magnetodb.storage import driver as storage_driver
from magnetodb.storage import models
from magnetodb.storage import table_info_repo

from magnetodb.storage.manager import simple_impl


class SimpleStorageManagerTestCase(unittest.TestCase):
    """The test for simple storage manager implementation."""

    def setUp(self):
        req_context.RequestContext()

    @mock.patch('magnetodb.storage.driver.StorageDriver.batch_write')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_schema')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_is_active')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_delete_item_async')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_put_item_async')
    def test_execute_write_batch(self, mock_put_item, mock_delete_item,
                                 mock_repo_get, mock_validate_table_is_active,
                                 mock_validate_table_schema, mock_batch_write):
        future = futures.Future()
        future.set_result(True)
        mock_put_item.return_value = future
        mock_delete_item.return_value = future
        mock_batch_write.side_effect = NotImplementedError()

        table_info = mock.Mock()
        table_info.schema.key_attributes = ['id', 'range']
        mock_repo_get.return_value = table_info

        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'

        request_map = {
            table_name: [
                models.WriteItemRequest.put(
                    {
                        'id': models.AttributeValue('N', 1),
                        'range': models.AttributeValue('S', '1'),
                        'str': models.AttributeValue('S', 'str1'),
                    }
                ),
                models.WriteItemRequest.put(
                    {
                        'id': models.AttributeValue('N', 2),
                        'range': models.AttributeValue('S', '1'),
                        'str': models.AttributeValue('S', 'str1')
                    }
                ),
                models.WriteItemRequest.delete(
                    {
                        'id': models.AttributeValue('N', 3),
                        'range': models.AttributeValue('S', '3')
                    }
                )
            ]
        }

        expected_put = [
            mock.call(
                context, table_info,
                {
                    'id': models.AttributeValue('N', 1),
                    'range': models.AttributeValue('S', '1'),
                    'str': models.AttributeValue('S', 'str1')
                }
            ),
            mock.call(
                context, table_info,
                {
                    'id': models.AttributeValue('N', 2),
                    'range': models.AttributeValue('S', '1'),
                    'str': models.AttributeValue('S', 'str1')
                }
            ),
        ]
        expected_delete = [
            mock.call(
                context, table_info,
                {
                    'id': models.AttributeValue('N', 3),
                    'range': models.AttributeValue('S', '3')
                }
            )
        ]

        storage_manager = simple_impl.SimpleStorageManager(
            storage_driver.StorageDriver(),
            table_info_repo.TableInfoRepository()
        )

        unprocessed_items = storage_manager.execute_write_batch(
            context, request_map
        )

        self.assertEqual(expected_put, mock_put_item.call_args_list)
        self.assertEqual(expected_delete,
                         mock_delete_item.call_args_list)

        self.assertEqual(unprocessed_items, {})

    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_schema')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_is_active')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_get_item_async')
    def test_execute_get_batch(self, mock_get_item, mock_repo_get,
                               mock_validate_table_is_active,
                               mock_validate_table_schema):
        future = futures.Future()
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

        storage_manager = simple_impl.SimpleStorageManager(
            None, table_info_repo.TableInfoRepository()
        )

        result, unprocessed_items = storage_manager.execute_get_batch(
            context, request_list
        )
        mock_get_item.has_calls(expected_get)
        self.assertEqual(unprocessed_items, [])

    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.update')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    def test_update_status_on_describe_for_creating_table(
            self, mock_repo_get, mock_repo_update):

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'
        storage_manager = simple_impl.SimpleStorageManager(
            None, table_info_repo.TableInfoRepository()
        )

        table_info = table_info_repo.TableInfo(
            table_name, None, None, models.TableMeta.TABLE_STATUS_CREATING)
        table_info.last_update_date_time = (
            timeutils.utcnow() - datetime.timedelta(0, 1000)
        )

        mock_repo_get.return_value = table_info

        table_meta = storage_manager.describe_table(context, table_name)

        self.assertEqual(
            table_meta.status, models.TableMeta.TABLE_STATUS_CREATE_FAILED)

    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.update')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    def test_update_status_on_describe_for_creating_table_negative(
            self, mock_repo_get, mock_repo_update):

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'
        storage_manager = simple_impl.SimpleStorageManager(
            None, table_info_repo.TableInfoRepository()
        )

        table_info = table_info_repo.TableInfo(
            table_name, None, None, models.TableMeta.TABLE_STATUS_CREATING)
        table_info.last_update_date_time = timeutils.utcnow()

        mock_repo_get.return_value = table_info

        table_meta = storage_manager.describe_table(context, table_name)

        self.assertEqual(
            table_meta.status, models.TableMeta.TABLE_STATUS_CREATING)

    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.update')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    def test_update_status_on_describe_for_deleting_table(
            self, mock_repo_get, mock_repo_update):

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'
        storage_manager = simple_impl.SimpleStorageManager(
            None, table_info_repo.TableInfoRepository()
        )

        table_info = table_info_repo.TableInfo(
            table_name, None, None, models.TableMeta.TABLE_STATUS_DELETING)

        table_info.last_update_date_time = (
            timeutils.utcnow() - datetime.timedelta(0, 1000)
        )

        mock_repo_get.return_value = table_info

        table_meta = storage_manager.describe_table(context, table_name)

        self.assertEqual(
            table_meta.status, models.TableMeta.TABLE_STATUS_DELETE_FAILED)

    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.update')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    def test_update_status_on_describe_for_deleting_table_negative(
            self, mock_repo_get, mock_repo_update):

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'
        storage_manager = simple_impl.SimpleStorageManager(
            None, table_info_repo.TableInfoRepository()
        )

        table_info = table_info_repo.TableInfo(
            table_name, None, None, models.TableMeta.TABLE_STATUS_DELETING)
        table_info.last_update_date_time = timeutils.utcnow()

        mock_repo_get.return_value = table_info

        table_meta = storage_manager.describe_table(context, table_name)

        self.assertEqual(
            table_meta.status, models.TableMeta.TABLE_STATUS_DELETING)

    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_schema')
    @mock.patch('magnetodb.storage.manager.simple_impl.SimpleStorageManager.'
                '_validate_table_is_active')
    @mock.patch('magnetodb.storage.table_info_repo.TableInfoRepository.get')
    def test_execute_write_batch_put_delete_same_item(
            self, mock_repo_get, mock_validate_table_is_active,
            mock_validate_table_schema):

        table_info = mock.Mock()
        table_info.schema.key_attributes = ['id', 'range']
        mock_repo_get.return_value = table_info

        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'

        request_map = {
            table_name: [
                models.WriteItemRequest.put(
                    {
                        'id': models.AttributeValue('N', 1),
                        'range': models.AttributeValue('S', '1'),
                        'str': models.AttributeValue('S', 'str1'),
                    }
                ),
                models.WriteItemRequest.delete(
                    {
                        'id': models.AttributeValue('N', 1),
                        'range': models.AttributeValue('S', '1')
                    }
                )
            ]
        }

        storage_manager = simple_impl.SimpleStorageManager(
            storage_driver.StorageDriver(),
            table_info_repo.TableInfoRepository()
        )

        with self.assertRaises(exception.ValidationError) as raises_cm:
            storage_manager.execute_write_batch(
                context, request_map
            )

        exc = raises_cm.exception
        self.assertIn("More than one", exc._error_string)
