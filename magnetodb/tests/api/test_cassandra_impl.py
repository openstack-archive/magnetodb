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
        query += " system_attrs map<text, blob>,"
        query += " system_attr_types map<text, text>,"
        query += " system_attr_exist map<text, text>,"
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


class TestCassandraItemCrud(TestCassandraBase):

    def test_delete_item(self):
        self._create_table()
        self._create_index()

        query = ("INSERT INTO {}.{} (user_id, user_range, user_indexed)"
                 "VALUES (1, '1', '1')").format(self.keyspace,
                                                self.table_name)

        self.SESSION.execute(query)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0].user_id)

        del_req = models.DeleteItemRequest(
            self.table_name, {'id': models.Condition.eq(1),
                              'range': models.Condition.eq('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(self.context, del_req)

        all = self._select_all()

        self.assertEqual(0, len(all))
