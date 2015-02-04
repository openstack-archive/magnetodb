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

import datetime
import mock
import unittest

from oslo_utils import timeutils

from magnetodb.common import exception
from magnetodb.storage import models
from magnetodb.storage import table_info_repo
from magnetodb.storage.table_info_repo import cassandra_impl

CassandraTableInfoRepository = cassandra_impl.CassandraTableInfoRepository


class CassandraTableInfoRepositoryTestCase(unittest.TestCase):
    """The test for Cassandra table info repository implementation."""

    def test_table_not_exist_exception_in_get_item(self):
        cluster_handler_mock = mock.Mock()
        cluster_handler_mock.execute_query.return_value = None
        table_repo = CassandraTableInfoRepository(cluster_handler_mock)
        context = mock.Mock(tenant='fake_tenant')

        with self.assertRaises(
                exception.TableNotExistsException) as raises_cm:
            table_repo.get(context, "nonexistenttable")

        ex = raises_cm.exception
        self.assertIn("Table 'nonexistenttable' does not exist", ex.message)

    def test_set_last_update_date_time_on_save(self):
        cluster_handler_mock = mock.Mock()
        cluster_handler_mock.execute_query.return_value = [{'[applied]': True}]
        table_repo = CassandraTableInfoRepository(cluster_handler_mock)
        context = mock.Mock(tenant='fake_tenant')

        table_schema = mock.Mock()
        table_schema.to_json.return_value = ''

        table_info = table_info_repo.TableInfo(
            'fake_table', '00000000-0000-0000-0000-000000000000', table_schema,
            models.TableMeta.TABLE_STATUS_CREATING
        )
        table_info.last_update_date_time = (
            timeutils.utcnow() - datetime.timedelta(0, 1000)
        )
        table_repo.save(context, table_info)

        seconds = (timeutils.utcnow() -
                   table_info.last_update_date_time).total_seconds()
        self.assertLess(seconds, 30)

    def test_set_last_update_date_time_on_update(self):
        cluster_handler_mock = mock.Mock()
        cluster_handler_mock.execute_query.return_value = [{'[applied]': True}]
        table_repo = CassandraTableInfoRepository(cluster_handler_mock)
        context = mock.Mock(tenant='fake_tenant')

        table_schema = mock.Mock()
        table_schema.to_json.return_value = ''

        table_info = table_info_repo.TableInfo(
            'fake_table', '00000000-0000-0000-0000-000000000000', table_schema,
            models.TableMeta.TABLE_STATUS_CREATING)
        table_info.last_update_date_time = (
            timeutils.utcnow() - datetime.timedelta(0, 1000)
        )
        table_repo.update(context, table_info)

        seconds = (timeutils.utcnow() -
                   table_info.last_update_date_time).total_seconds()
        self.assertLess(seconds, 30)
