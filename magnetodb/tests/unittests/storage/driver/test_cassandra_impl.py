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
        cluster_handler = mock.Mock()
        cluster_handler.execute_query = mock_execute

        return cassandra_impl.CassandraStorageDriver(cluster_handler, {})

    @mock.patch('magnetodb.storage.driver.cassandra.'
                'cassandra_impl.CassandraStorageDriver.select_item')
    def test_update_item_delete_no_val(self, mock_select_item):
        mock_execute_query = mock.Mock(return_value=[{'[applied]': True}])
        mock_table_schema = models.TableSchema(
            key_attributes=['hash_key', 'range_key'],
            attribute_type_map={'hash_key': None,
                                'range_key': None,
                                'Tags': None},
            index_def_map=None
        )
        driver = self.get_connection(mock_execute_query, mock_table_schema)

        value = models.AttributeValue('SS', {"Update", "Help"})
        mock_select_item.return_value = mock.Mock(items=[{'Tags': value}])

        context = mock.Mock(tenant='fake_tenant')
        table_info = mock.Mock(
            schema=mock_table_schema,
            internal_name='"u_fake_tenant"."u_fake_table"'
        )

        key_attrs = {
            'hash_key': models.AttributeValue('N', 1),
            'range_key': models.AttributeValue('S', 'two')
        }
        attr_actions = {
            'Tags': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE,
                None
            )
        }

        driver.update_item(context, table_info, key_attrs, attr_actions)

        expected_calls = [
            mock.call('UPDATE "u_fake_tenant"."u_fake_table" SET '
                      '"u_Tags"=null WHERE "u_hash_key"=1 AND '
                      '"u_range_key"=\'two\' '
                      'IF "u_Tags"={\'Help\',\'Update\'}', consistent=True)
        ]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)

    @mock.patch('magnetodb.storage.driver.cassandra.'
                'cassandra_impl.CassandraStorageDriver.select_item')
    def test_update_item_delete_set(self, mock_select_item):
        mock_execute_query = mock.Mock(return_value=[{'[applied]': True}])
        mock_table_schema = models.TableSchema(
            key_attributes=['hash_key', 'range_key'],
            attribute_type_map={'hash_key': None,
                                'range_key': None,
                                'Tags': None},
            index_def_map=None
        )
        driver = self.get_connection(mock_execute_query, mock_table_schema)

        value = models.AttributeValue('SS', {"Update", "Help"})
        mock_select_item.return_value = mock.Mock(items=[{'Tags': value}])

        context = mock.Mock(tenant='fake_tenant')
        table_info = mock.Mock(
            schema=mock_table_schema,
            internal_name='"u_fake_tenant"."u_fake_table"'
        )

        key_attrs = {
            'hash_key': models.AttributeValue('N', 1),
            'range_key': models.AttributeValue('S', 'two')
        }
        attr_actions = {
            'Tags': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE,
                models.AttributeValue('SS', {"Update"})
            )
        }

        driver.update_item(context, table_info, key_attrs, attr_actions)

        expected_calls = [
            mock.call('UPDATE "u_fake_tenant"."u_fake_table" SET '
                      '"u_Tags"={\'Help\'} WHERE "u_hash_key"=1 AND '
                      '"u_range_key"=\'two\' '
                      'IF "u_Tags"={\'Help\',\'Update\'}', consistent=True)
        ]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)

    @mock.patch('magnetodb.storage.driver.cassandra.'
                'cassandra_impl.CassandraStorageDriver.select_item')
    def test_update_item_add_number(self, mock_select_item):
        mock_execute_query = mock.Mock(return_value=[{'[applied]': True}])
        mock_table_schema = models.TableSchema(
            key_attributes=['hash_key', 'range_key'],
            attribute_type_map={'hash_key': None,
                                'range_key': None,
                                'ViewsCount': None},
            index_def_map=None
        )
        driver = self.get_connection(mock_execute_query, mock_table_schema)

        def make_select_result(i):
            value = models.AttributeValue('N', i)
            return mock.Mock(items=[{'ViewsCount': value}])

        values = [make_select_result(i) for i in range(10)]
        mock_select_item.side_effect = values

        context = mock.Mock(tenant='fake_tenant')
        table_info = mock.Mock(
            schema=mock_table_schema,
            internal_name='"u_fake_tenant"."u_fake_table"'
        )

        key_attrs = {
            'hash_key': models.AttributeValue('N', 1),
            'range_key': models.AttributeValue('S', 'two')
        }
        attr_actions = {
            'ViewsCount': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_ADD,
                models.AttributeValue('N', 1)
            )
        }

        for i in range(10):
            driver.update_item(context, table_info, key_attrs, attr_actions)

        expected_calls = [
            mock.call('UPDATE "u_fake_tenant"."u_fake_table" SET '
                      '"u_ViewsCount"=%d WHERE "u_hash_key"=1 AND '
                      '"u_range_key"=\'two\' '
                      'IF "u_ViewsCount"=%d' % (i, i - 1),
                      consistent=True) for i in range(1, 11)
        ]

        self.assertEqual(expected_calls, mock_execute_query.mock_calls)
