# Copyright 2014 Symantec Inc.
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
from magnetodb.storage.driver.cassandra import cassandra_impl


class CassandraDriverTestCase(unittest.TestCase):
    """The test for Cassandra storage implementation."""

    def test_update_counter_item(self):
        table_schema = mock.Mock(key_attributes=['hash_key', 'range_key'])
        table_info = mock.Mock(
            schema=table_schema,
            internal_counter_table_name='counter_fake_table',
            internal_keyspace='user_fake_tenant')
        table_repo = mock.Mock()
        table_repo.get = mock.Mock(return_value=table_info)

        mock_execute_query = mock.Mock(return_value=None)
        cluster_handler = mock.Mock()
        cluster_handler.execute_query = mock_execute_query

        driver = cassandra_impl.CassandraStorageDriver(cluster_handler,
                                                       table_repo)
        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'
        key_attrs = {'hash_key': models.AttributeValue.number(1),
                     'range_key': models.AttributeValue.str('two')}

        driver.update_counter_item(context, table_name, key_attrs,
                                   {'counter_1': 1, 'counter2': -2})

        expected_calls = [
            mock.call('UPDATE "user_fake_tenant"."counter_fake_table" '
                      'SET "counter_1" = "counter_1" + 1, '
                      '"counter2" = "counter2" + -2  WHERE '
                      '"user_hash_key"=1 AND "user_range_key"=\'two\'')]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)

    def test_get_counter_item(self):
        table_schema = mock.Mock(key_attributes=['hash_key', 'range_key'])
        table_info = mock.Mock(
            schema=table_schema,
            internal_counter_table_name='counter_fake_table',
            internal_keyspace='user_fake_tenant')
        table_repo = mock.Mock()
        table_repo.get = mock.Mock(return_value=table_info)

        mock_execute_query = mock.Mock(return_value=[{'counter_1': 1,
                                                      'counter2': 2}])
        cluster_handler = mock.Mock()
        cluster_handler.execute_query = mock_execute_query

        driver = cassandra_impl.CassandraStorageDriver(cluster_handler,
                                                       table_repo)
        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'
        key_attrs = {'hash_key': models.AttributeValue.number(1),
                     'range_key': models.AttributeValue.str('two')}

        driver.get_counter_item(context, table_name, key_attrs)

        expected_calls = [
            mock.call('SELECT * FROM "user_fake_tenant"."counter_fake_table" '
                      'WHERE "user_hash_key"=1 AND "user_range_key"=\'two\'',
                      False)]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)

    def test_get_counter_item_counters_specified(self):
        table_schema = mock.Mock(key_attributes=['hash_key', 'range_key'])
        table_info = mock.Mock(
            schema=table_schema,
            internal_counter_table_name='counter_fake_table',
            internal_keyspace='user_fake_tenant')
        table_repo = mock.Mock()
        table_repo.get = mock.Mock(return_value=table_info)

        mock_execute_query = mock.Mock(return_value=[{'counter_1': 1,
                                                      'counter2': 2}])
        cluster_handler = mock.Mock()
        cluster_handler.execute_query = mock_execute_query

        driver = cassandra_impl.CassandraStorageDriver(cluster_handler,
                                                       table_repo)
        context = mock.Mock(tenant='fake_tenant')

        table_name = 'fake_table'
        key_attrs = {'hash_key': models.AttributeValue.number(1),
                     'range_key': models.AttributeValue.str('two')}

        driver.get_counter_item(context, table_name, key_attrs,
                                ['counter_1', 'counter2'])

        expected_calls = [
            mock.call('SELECT "counter_1", "counter2" FROM '
                      '"user_fake_tenant"."counter_fake_table" '
                      'WHERE "user_hash_key"=1 AND "user_range_key"=\'two\'',
                      False)]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)
