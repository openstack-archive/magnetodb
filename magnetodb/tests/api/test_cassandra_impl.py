# Copyright 2013 Mirantis Inc.
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

import unittest
import uuid

from cassandra import cluster

from magnetodb.storage import models
from magnetodb.storage.impl import cassandra_impl


TEST_CONNECTION = {'contact_points': ("localhost",)}


class FakeContext(object):
    def __init__(self, tenant):
        self.tenant = tenant


class TestCassandraBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCassandraBase, cls).setUpClass()

        cls.CASANDRA_STORAGE_IMPL = cassandra_impl.CassandraStorageImpl(
            **TEST_CONNECTION)

        cls.CLUSTER = cluster.Cluster(**TEST_CONNECTION)
        cls.SESSION = cls.CLUSTER.connect()

    @classmethod
    def tearDownClass(cls):
        super(TestCassandraBase, cls).tearDownClass()

    def setUp(self):
        self.keyspace = self._get_unique_name()

        self._create_keyspace()

        self.context = FakeContext(self.keyspace)

        self.table_name = self._get_unique_name()

    def tearDown(self):
        self._drop_keyspace()

    @staticmethod
    def _get_unique_name():
        name = str(uuid.uuid4())
        return 'test' + filter(lambda x: x != '-', name)[:28]

    def _create_keyspace(self, keyspace=None):
        keyspace = keyspace or self.keyspace

        query = "CREATE KEYSPACE {} WITH replication".format(keyspace)
        query += " = {'class':'SimpleStrategy', 'replication_factor':1}"

        self.SESSION.execute(query)

    def _drop_keyspace(self, keyspace=None):
        keyspace = keyspace or self.keyspace

        query = ("DROP KEYSPACE {}".format(keyspace))

        self.SESSION.execute(query)

    def _get_table_names(self, keyspace=None):
        keyspace = keyspace or self.keyspace

        ks = self.CLUSTER.metadata.keyspaces[keyspace]

        return ks.tables.keys()

    def _create_table(self, keyspace=None, table_name=None):
        keyspace = keyspace or self.keyspace
        table_name = table_name or self.table_name
        query = "CREATE TABLE {}.{} (".format(keyspace, table_name)
        query += " user_id decimal,"
        query += " user_range text,"
        query += " user_indexed text,"
        query += " user_nonindexed text,"
        query += " system_attrs map<text, blob>,"
        query += " system_attr_types map<text, text>,"
        query += " system_attr_exist set<text>,"
        query += " PRIMARY KEY(user_id, user_range))"
        self.SESSION.execute(query)

    def _create_index(self, keyspace=None, table_name=None,
                      attr='user_indexed'):
        keyspace = keyspace or self.keyspace
        table_name = table_name or self.table_name
        query = "CREATE INDEX ON {}.{} ({})".format(
            keyspace, table_name, attr)
        self.SESSION.execute(query)

    def _drop_table(self, keyspace=None, table_name=None):
        keyspace = keyspace or self.keyspace
        table_name = None or self.table_name
        query = "DROP TABLE IF EXISTS {}.{}".format(keyspace, table_name)
        self.SESSION.execute(query)

    def _select_all(self, keyspace=None, table_name=None):
        keyspace = keyspace or self.keyspace
        table_name = None or self.table_name
        query = "SELECT * FROM {}.{}".format(keyspace, table_name)
        return self.SESSION.execute(query)


class TestCassandraTableCrud(TestCassandraBase):

    def test_create_table(self):
        self.assertEqual([], self._get_table_names())

        attrs = {
            models.AttributeDefinition(
                'id', models.ATTRIBUTE_TYPE_NUMBER),
            models.AttributeDefinition(
                'range', models.ATTRIBUTE_TYPE_STRING),
            models.AttributeDefinition(
                'nonindexed', models.ATTRIBUTE_TYPE_STRING),
            models.AttributeDefinition(
                'indexed', models.ATTRIBUTE_TYPE_STRING)
        }

        schema = models.TableSchema(self.table_name, attrs, {'id', 'range'},
                                    {'indexed'})

        self.CASANDRA_STORAGE_IMPL.create_table(self.context, schema)

        self.assertEqual([self.table_name], self._get_table_names())

    def test_list_table(self):
        self.assertEqual(
            [], self.CASANDRA_STORAGE_IMPL.list_tables(self.context))

        self._create_table()

        self.assertEqual([self.table_name],
                         self.CASANDRA_STORAGE_IMPL.list_tables(self.context))

    def test_describe_table(self):
        attrs = {
            models.AttributeDefinition(
                'id', models.ATTRIBUTE_TYPE_NUMBER),
            models.AttributeDefinition(
                'range', models.ATTRIBUTE_TYPE_STRING),
            models.AttributeDefinition(
                'nonindexed', models.ATTRIBUTE_TYPE_STRING),
            models.AttributeDefinition(
                'indexed', models.ATTRIBUTE_TYPE_STRING)
        }

        schema = models.TableSchema(self.table_name, attrs, {'id', 'range'},
                                    {'indexed'})

        self._create_table()
        self._create_index()

        desc = self.CASANDRA_STORAGE_IMPL.describe_table(
            self.context, self.table_name)

        self.assertEqual(schema, desc)

    def test_delete_table(self):
        self._create_table()

        self.assertEqual([self.table_name], self._get_table_names())

        self.CASANDRA_STORAGE_IMPL.delete_table(self.context, self.table_name)

        self.assertEqual([], self._get_table_names())


class TestCassandraDeleteItem(TestCassandraBase):

    def test_delete_item_where(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range,"
                 " user_nonindexed, user_indexed)"
                 " VALUES (1, '1', '1', '1')").format(self.keyspace,
                                                      self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.Condition.eq(1), 'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req)

        all = self._select_all()

        self.assertEqual(0, len(all))

    def test_delete_item_where_negative(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range,"
                 " user_nonindexed, user_indexed)"
                 " VALUES (1, '2', '1', '1')").format(self.keyspace,
                                                      self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.Condition.eq(1), 'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

    @unittest.skip("conditional updates noy yet implemented")
    def test_delete_item_if_exists(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range,"
                 " user_nonindexed, user_indexed)"
                 " VALUES (1, '1', '1', '1')").format(self.keyspace,
                                                      self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        expected = {'nonindexed': models.ExpectedCondition.exists()}

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.Condition.eq(1), 'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req, expected)

        all = self._select_all()

        self.assertEqual(0, len(all))

    @unittest.skip("conditional updates noy yet implemented")
    def test_delete_item_if_exists_negative(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range,"
                 " user_nonindexed, user_indexed)"
                 " VALUES (1, '1', null, '1')").format(self.keyspace,
                                                       self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        expected = {'nonindexed': models.ExpectedCondition.exists()}

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.Condition.eq(1), 'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req, expected)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

    @unittest.skip("conditional updates noy yet implemented")
    def test_delete_item_if_not_exists(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range,"
                 " user_nonindexed, user_indexed)"
                 " VALUES (1, '1', null, '1')").format(self.keyspace,
                                                       self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        expected = {'nonindexed': models.ExpectedCondition.not_exists()}

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.Condition.eq(1), 'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req, expected)

        all = self._select_all()

        self.assertEqual(0, len(all))

    @unittest.skip("conditional updates noy yet implemented")
    def test_delete_item_if_not_exists_negative(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range,"
                 " user_nonindexed, user_indexed)"
                 " VALUES (1, '1', '1', '1')").format(self.keyspace,
                                                      self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        expected = {'nonindexed': models.ExpectedCondition.not_exists()}

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.Condition.eq(1), 'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req, expected)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)


class TestCassandraSelectItem(TestCassandraBase):

    def _insert_data(self):
        query = "UPDATE {}.{}".format(self.keyspace, self.table_name)
        query += " SET user_indexed='1', user_nonindexed='1',"
        query += " {}['{}'] = textAsBlob('{}'), ".format(
            self.CASANDRA_STORAGE_IMPL.SYSTEM_COLUMN_ATTRS,
            'field', 'value')
        query += " {}['{}']= '{}', ".format(
            self.CASANDRA_STORAGE_IMPL.SYSTEM_COLUMN_ATTR_TYPES,
            'field', self.CASANDRA_STORAGE_IMPL.STORAGE_TO_CASSANDRA_TYPES[
                models.ATTRIBUTE_TYPE_STRING])
        query += " {} = {} + {{ '{}','{}','{}','{}','{}' }} ".format(
            self.CASANDRA_STORAGE_IMPL.SYSTEM_COLUMN_ATTR_EXIST,
            self.CASANDRA_STORAGE_IMPL.SYSTEM_COLUMN_ATTR_EXIST,
            'id', 'range', 'indexed', 'nonindexed', 'field')
        query += " WHERE user_id = 1 AND user_range='1'"

        self.SESSION.execute(query)

    def _validate_data(self, data):

        expected = {}

        expected['id'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_NUMBER, 1)

        expected['range'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING, '1')

        expected['indexed'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING, '1')

        expected['nonindexed'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING, '1')

        expected['field'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING, 'value')

        self.assertDictEqual(expected, data)

    def test_select_item(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.Condition.eq('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, len(result))
        self._validate_data(result[0])

    def test_select_item_attr(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.Condition.eq('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond, ['field'])

        self.assertEqual(1, len(result))
        self.assertEqual(
            {'field': models.AttributeValue(
                models.ATTRIBUTE_TYPE_STRING, 'value')},
            result[0])

    def test_select_item_negative(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.Condition.eq('2')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, len(result))

    def test_select_item_less(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.lt('2')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, len(result))
        self._validate_data(result[0])

    def test_select_item_less_negative(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.lt('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, len(result))

    def test_select_item_less_eq(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.le('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, len(result))
        self._validate_data(result[0])

    def test_select_item_less_eq_negative(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.le('0')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, len(result))

    def test_select_item_greater(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.gt('0')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, len(result))
        self._validate_data(result[0])

    def test_select_item_greater_negative(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.gt('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, len(result))

    def test_select_item_greater_eq(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.ge('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, len(result))
        self._validate_data(result[0])

    def test_select_item_greater_eq_negative(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.IndexedCondition.ge('2')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, len(result))

    def test_select_item_indexed(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.Condition.eq('1'),
                        'indexed': models.Condition.eq('1')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, len(result))
        self._validate_data(result[0])

    def test_select_item_indexed_negative(self):
        self._create_table()
        self._create_index()

        self._insert_data()

        indexed_cond = {'id': models.Condition.eq(1),
                        'range': models.Condition.eq('1'),
                        'indexed': models.Condition.eq('2')}

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, len(result))
