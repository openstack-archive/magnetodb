# Copyright 2014 Symantec Corporation.
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
from magnetodb.storage.driver import StorageDriver
from magnetodb.storage.models import WriteItemRequest
from magnetodb.storage.table_info_repo import TableInfoRepository
import mock
import time

from concurrent.futures import Future

from magnetodb.tests.unittests.common.notifier.test_notification \
    import TestNotify
from magnetodb.tests.unittests.common.notifier.test_notification \
    import DATETIMEFORMAT

from magnetodb import notifier

from magnetodb.storage import models
from magnetodb.storage.manager.simple_impl import SimpleStorageManager
from magnetodb.storage.manager.async_simple_impl \
    import AsyncSimpleStorageManager


class TestNotifyStorageManager(TestNotify):
    """Unit tests for event notifier in Storage Manager."""

    @mock.patch('magnetodb.storage.table_info_repo')
    def test_notify_create_table_async(self, mock_table_info_repo):
        self.cleanup_test_notifier()

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'
        table_schema = 'fake_table_schema'

        mock_storage_driver = mock.Mock()
        mock_storage_driver.create_table.return_value = True

        storage_manager = AsyncSimpleStorageManager(mock_storage_driver,
                                                    mock_table_info_repo)
        storage_manager.create_table(context, table_name, table_schema)

        # wait for async create table call to finish
        for i in range(10):
            if (mock_table_info_repo.update.called and
                    len(self.get_notifications()) == 2):
                break
            else:
                time.sleep(1)
        else:
            self.fail("Couldn't wait for async request completion")

        # create_table method has been called
        self.assertTrue(mock_storage_driver.create_table.called)

        # check notification queue
        self.assertEqual(len(self.get_notifications()), 2)

        start_event = self.get_notifications()[0]
        end_event = self.get_notifications()[1]

        self.assertEqual(start_event['priority'], 'INFO')
        self.assertEqual(start_event['event_type'],
                         notifier.EVENT_TYPE_TABLE_CREATE_START)
        self.assertEqual(start_event['payload'], table_schema)

        self.assertEqual(end_event['priority'], 'INFO')
        self.assertEqual(end_event['event_type'],
                         notifier.EVENT_TYPE_TABLE_CREATE_END)
        self.assertEqual(end_event['payload'], table_schema)

        time_start = datetime.datetime.strptime(
            start_event['timestamp'], DATETIMEFORMAT)
        time_end = datetime.datetime.strptime(
            end_event['timestamp'], DATETIMEFORMAT)
        self.assertTrue(time_start < time_end,
                        "start event is later than end event")

    @mock.patch('magnetodb.storage.table_info_repo')
    def test_notify_delete_table_async(self, mock_table_info_repo):
        self.cleanup_test_notifier()

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'

        mock_storage_driver = mock.Mock()
        mock_storage_driver.delete_table.return_value = True

        class FakeTableInfo:
            status = models.TableMeta.TABLE_STATUS_ACTIVE
            name = table_name
            id = None
            schema = None
            creation_date_time = None

        mock_table_info_repo = mock.Mock()
        mock_table_info_repo.get.return_value = FakeTableInfo()

        storage_manager = AsyncSimpleStorageManager(mock_storage_driver,
                                                    mock_table_info_repo)
        storage_manager.delete_table(context, table_name)

        # wait for async delete table call to finish
        for i in range(10):
            if (mock_table_info_repo.delete.called and
                    len(self.get_notifications()) == 2):
                # delete_table method of mock_storage_driver has been called
                break
            else:
                time.sleep(1)
        else:
            self.fail("Couldn't wait for async request completion")

        # delete method of mock_table_info_repo has been called
        self.assertTrue(mock_table_info_repo.delete.called)

        # check notification queue
        self.assertEqual(len(self.get_notifications()), 2)

        start_event = self.get_notifications()[0]
        end_event = self.get_notifications()[1]

        self.assertEqual(start_event['priority'], 'INFO')
        self.assertEqual(start_event['event_type'],
                         notifier.EVENT_TYPE_TABLE_DELETE_START)
        self.assertEqual(start_event['payload'], table_name)

        self.assertEqual(end_event['priority'], 'INFO')
        self.assertEqual(end_event['event_type'],
                         notifier.EVENT_TYPE_TABLE_DELETE_END)
        self.assertEqual(end_event['payload'], table_name)

        time_start = datetime.datetime.strptime(
            start_event['timestamp'], DATETIMEFORMAT)
        time_end = datetime.datetime.strptime(
            end_event['timestamp'], DATETIMEFORMAT)
        self.assertTrue(time_start < time_end,
                        "start event is later than end event")

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
    def test_notify_batch_write(self, mock_put_item, mock_delete_item,
                                mock_repo_get, mock_validate_table_is_active,
                                mock_validate_table_schema, mock_batch_write):
        self.cleanup_test_notifier()

        future = Future()
        future.set_result(True)
        mock_put_item.return_value = future
        mock_delete_item.return_value = future

        table_info = mock.Mock()
        table_info.schema.key_attributes = ['id', 'range']
        mock_repo_get.return_value = table_info

        mock_batch_write.side_effect = NotImplementedError()

        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'

        request_map = {
            table_name: [
                WriteItemRequest.put(
                    {
                        'id': models.AttributeValue('N', 1),
                        'range': models.AttributeValue('S', '1'),
                        'str': models.AttributeValue('S', 'str1'),
                    }
                ),
                WriteItemRequest.put(
                    {
                        'id': models.AttributeValue('N', 2),
                        'range': models.AttributeValue('S', '1'),
                        'str': models.AttributeValue('S', 'str1')
                    }
                ),
                WriteItemRequest.delete(
                    {
                        'id': models.AttributeValue('N', 3),
                        'range': models.AttributeValue('S', '3')
                    }
                )
            ]
        }

        storage_manager = SimpleStorageManager(
            StorageDriver(), TableInfoRepository()
        )
        storage_manager.execute_write_batch(context, request_map)

        # check notification queue
        self.assertEqual(len(self.get_notifications()), 2)

        start_event = self.get_notifications()[0]
        end_event = self.get_notifications()[1]

        self.assertEqual(start_event['priority'], 'INFO')
        self.assertEqual(start_event['event_type'],
                         notifier.EVENT_TYPE_DATA_BATCHWRITE_START)
        self.assertEqual(len(start_event['payload']), len(request_map))

        self.assertEqual(end_event['priority'], 'INFO')
        self.assertEqual(end_event['event_type'],
                         notifier.EVENT_TYPE_DATA_BATCHWRITE_END)
        self.assertEqual(len(end_event['payload']['write_request_map']),
                         len(request_map))
        self.assertEqual(len(end_event['payload']['unprocessed_items']), 0)

        time_start = datetime.datetime.strptime(
            start_event['timestamp'], DATETIMEFORMAT)
        time_end = datetime.datetime.strptime(
            end_event['timestamp'], DATETIMEFORMAT)
        self.assertTrue(time_start < time_end,
                        "start event is later than end event")
