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
    """The test for Cassandra driver."""

    def get_connection(self, mock_execute, mock_table_schema):
        table_info = mock.Mock(
            schema=mock_table_schema,
            internal_keyspace='user_fake_tenant',
            internal_name='user_fake_table'
        )

        table_repo = mock.Mock()
        table_repo.get = mock.Mock(return_value=table_info)

        cluster_handler = mock.Mock()
        cluster_handler.execute_query = mock_execute

        return cassandra_impl.CassandraStorageDriver(cluster_handler,
                                                     table_repo)

    @mock.patch('magnetodb.storage.driver.cassandra.'
                'cassandra_impl.CassandraStorageDriver.select_item')
    def test_update_item_delete_no_val(self, mock_select_item):
        mock_execute_query = mock.Mock(return_value=None)
        mock_table_schema = mock.Mock(
            key_attributes=['hash_key', 'range_key'],
            attribute_type_map={'hash_key': None,
                                'range_key': None,
                                'Tags': None},
            index_def_map=None
        )
        driver = self.get_connection(mock_execute_query, mock_table_schema)

        value = models.AttributeValue.str_set(["Update", "Help"])
        mock_select_item.return_value = mock.Mock(items=[{'Tags': value}])

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'

        key_attrs = {
            'hash_key': models.AttributeValue.number(1),
            'range_key': models.AttributeValue.str('two')
        }
        attr_actions = {
            'Tags': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE,
                None
            )
        }

        driver.update_item(context, table_name, key_attrs, attr_actions)

        expected_calls = [
            mock.call('UPDATE "user_fake_tenant"."user_fake_table" SET '
                      '"user_Tags"=null WHERE "user_hash_key"=1 AND '
                      '"user_range_key"=\'two\'', consistent=True)
        ]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)

    @mock.patch('magnetodb.storage.driver.cassandra.'
                'cassandra_impl.CassandraStorageDriver.select_item')
    def test_update_item_delete_set(self, mock_select_item):
        mock_execute_query = mock.Mock(return_value=None)
        mock_table_schema = mock.Mock(
            key_attributes=['hash_key', 'range_key'],
            attribute_type_map={'hash_key': None,
                                'range_key': None,
                                'Tags': None},
            index_def_map=None
        )
        driver = self.get_connection(mock_execute_query, mock_table_schema)

        value = models.AttributeValue.str_set(["Update", "Help"])
        mock_select_item.return_value = mock.Mock(items=[{'Tags': value}])

        context = mock.Mock(tenant='fake_tenant')
        table_name = 'fake_table'

        key_attrs = {
            'hash_key': models.AttributeValue.number(1),
            'range_key': models.AttributeValue.str('two')
        }
        attr_actions = {
            'Tags': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE,
                models.AttributeValue.str_set(["Update"])
            )
        }

        driver.update_item(context, table_name, key_attrs, attr_actions)

        expected_calls = [
            mock.call('UPDATE "user_fake_tenant"."user_fake_table" SET '
                      '"user_Tags"={\'Help\'} WHERE "user_hash_key"=1 AND '
                      '"user_range_key"=\'two\' '
                      'IF "user_Tags"={\'Help\',\'Update\'}', consistent=True)
        ]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)
