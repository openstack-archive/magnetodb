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

import mock
import time

from magnetodb import notifier
from magnetodb.storage.manager import async_simple_impl
from magnetodb.storage import models
from magnetodb.tests.unittests.common.notifier import test_notification

DATETIMEFORMAT = test_notification.DATETIMEFORMAT


class TestNotifyStorageManager(test_notification.TestNotify):
    """Unit tests for event notifier in Storage Manager."""

    @mock.patch('magnetodb.storage.table_info_repo')
    def test_notify_create_table_async(self, mock_table_info_repo):
        self.cleanup_test_notifier()

        tenant = 'fake_tenant'
        table_name = 'fake_table'
        table_schema = 'fake_table_schema'

        mock_storage_driver = mock.Mock()
        mock_storage_driver.create_table.return_value = True

        storage_manager = async_simple_impl.AsyncSimpleStorageManager(
            mock_storage_driver, mock_table_info_repo)
        storage_manager.create_table(tenant, table_name, table_schema)

        # wait for async create table call to finish
        for i in range(10):
            if (mock_table_info_repo.update.called and
                    len(self.get_notifications()) == 1):
                break
            else:
                time.sleep(1)
        else:
            self.fail("Couldn't wait for async request completion")

        # create_table method has been called
        self.assertTrue(mock_storage_driver.create_table.called)

        # check notification queue
        self.assertEqual(len(self.get_notifications()), 1)

        end_event = self.get_notifications()[0]

        self.assertEqual(end_event['priority'], 'INFO')
        self.assertEqual(end_event['event_type'],
                         notifier.EVENT_TYPE_TABLE_CREATE)
        self.assertEqual(end_event['payload']['schema'], table_schema)

    @mock.patch('magnetodb.storage.table_info_repo')
    def test_notify_delete_table_async(self, mock_table_info_repo):
        self.cleanup_test_notifier()

        tenant = 'fake_tenant'
        table_name = 'fake_table'

        mock_storage_driver = mock.Mock()
        mock_storage_driver.delete_table.return_value = True

        class FakeTableInfo:
            status = models.TableMeta.TABLE_STATUS_ACTIVE
            name = table_name
            in_use = False
            id = None
            schema = None
            creation_date_time = None
            internal_name = None

        mock_table_info_repo = mock.Mock()
        mock_table_info_repo.get.return_value = FakeTableInfo()

        storage_manager = async_simple_impl.AsyncSimpleStorageManager(
            mock_storage_driver, mock_table_info_repo)
        storage_manager.delete_table(tenant, table_name)

        # wait for async delete table call to finish
        for i in range(10):
            if (mock_table_info_repo.delete.called and
                    len(self.get_notifications()) == 1):
                # delete_table method of mock_storage_driver has been called
                break
            else:
                time.sleep(1)
        else:
            self.fail("Couldn't wait for async request completion")

        # delete method of mock_table_info_repo has been called
        self.assertTrue(mock_table_info_repo.delete.called)

        # check notification queue
        self.assertEqual(len(self.get_notifications()), 1)

        start_event = self.get_notifications()[0]

        self.assertEqual(start_event['priority'], 'INFO')
        self.assertEqual(start_event['event_type'],
                         notifier.EVENT_TYPE_TABLE_DELETE)
        self.assertEqual(start_event['payload']['table_name'], table_name)
