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

from copy import deepcopy

import mock
import time
import unittest

from magnetodb.storage import models
from magnetodb.storage.manager.async_simple_impl import (
    AsyncSimpleStorageManager
)


class AsyncStorageManagerTestCase(unittest.TestCase):
    """The test for async storage manager implementation."""

    @mock.patch('magnetodb.storage.table_info_repo')
    def test_create_table_async(self, mock_table_info_repo):
        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'
        table_schema = 'fake_table_schema'

        mock_storage_driver = mock.Mock()
        mock_storage_driver.create_table.return_value = "fake_internal_name"

        table_info_save_args_list = []

        def side_effect(*args):
            table_info_save_args_list.append(deepcopy(args))

        mock_table_info_repo.save.side_effect = side_effect

        storage_manager = AsyncSimpleStorageManager(mock_storage_driver,
                                                    mock_table_info_repo)
        storage_manager.create_table(context, table_name, table_schema)

        # called once, length of call_args_list indicates number of calls
        self.assertEqual(1, len(table_info_save_args_list))

        # CallList is tuple of Mock and TableInfo
        self.assertEqual(2, len(table_info_save_args_list[0]))

        # TableInfo status should be creating initially when
        # table_info_repo.save is called
        self.assertEquals(models.TableMeta.TABLE_STATUS_CREATING,
                          table_info_save_args_list[0][1].status)

        # wait for async create table call to finish
        for i in range(10):
            if mock_table_info_repo.update.called:
                table_info_update_args_list = (
                    mock_table_info_repo.update.call_args_list
                )

                # called once
                # length of call_args_list indicates number of calls
                self.assertEqual(1, len(table_info_update_args_list))

                self.assertEqual(2, len(table_info_update_args_list[0]))

                # tuple of Mock, TableInfo, and status list
                self.assertEqual(3, len(table_info_update_args_list[0][0]))

                # TableInfo status should be active by now
                self.assertEqual(models.TableMeta.TABLE_STATUS_ACTIVE,
                                 table_info_update_args_list[0][0][1].status)
                break
            else:
                time.sleep(1)

        # create_table method has been called
        self.assertTrue(mock_storage_driver.create_table.called)

    @mock.patch('magnetodb.storage.table_info_repo')
    def test_delete_table_async(self, mock_table_info_repo):
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

        table_info_update_args_list = (
            mock_table_info_repo.update.call_args_list
        )

        # called once, length of call_args_list indicates number of calls
        self.assertEqual(1, len(table_info_update_args_list))

        self.assertEqual(2, len(table_info_update_args_list[0]))

        # tuple of Mock, TableInfo, and status list
        self.assertEqual(3, len(table_info_update_args_list[0][0]))

        # TableInfo status should be deleting
        self.assertEqual(models.TableMeta.TABLE_STATUS_DELETING,
                         table_info_update_args_list[0][0][1].status)

        for i in range(10):
            if mock_table_info_repo.delete.called:
                # delete_table method of mock_storage_driver has been called
                self.assertTrue(mock_storage_driver.delete_table.called)
                break
            else:
                time.sleep(1)

        # delete method of mock_table_info_repo has been called
        self.assertTrue(mock_table_info_repo.delete.called)
