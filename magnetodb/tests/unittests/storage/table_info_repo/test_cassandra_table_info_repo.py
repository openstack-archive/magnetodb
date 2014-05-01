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

from magnetodb.common import exception

from magnetodb.storage.table_info_repo.cassandra_impl import (
    CassandraTableInfoRepository
)


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
