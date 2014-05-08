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
import json

import unittest
import uuid
import binascii
from magnetodb.common import exception

from cassandra import cluster
from cassandra import query
from magnetodb.common.cassandra.cluster_handler import ClusterHandler
from magnetodb.storage import models
from magnetodb.storage.driver.cassandra import (
    cassandra_impl,
    SYSTEM_COLUMN_INDEX_NAME,
    SYSTEM_COLUMN_INDEX_VALUE_STRING,
    SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
    SYSTEM_COLUMN_INDEX_VALUE_BLOB,
    USER_PREFIX,
    SYSTEM_COLUMN_EXTRA_ATTR_DATA,
    SYSTEM_COLUMN_EXTRA_ATTR_TYPES,
    SYSTEM_COLUMN_ATTR_EXIST
)
from magnetodb.storage.manager.simple_impl import SimpleStorageManager
from magnetodb.storage.table_info_repo.cassandra_impl import (
    CassandraTableInfoRepository
)

TEST_CONNECTION = {
    'contact_points': ("localhost",),
    'control_connection_timeout': 60
}


class FakeContext(object):
    def __init__(self, tenant):
        self.tenant = tenant


class TestCassandraBase(unittest.TestCase):
    TENANT_PER_TEST_METHOD = "test"
    TENANT_PER_TEST_CLASS = "class"

    _tenant_scope = TENANT_PER_TEST_CLASS

    test_data_keys = {
        'id': ('decimal', '1', 1),
        'range': ('text', "'1'", '1')
    }

    test_table_schema = models.TableSchema(
        {
            "id": models.ATTRIBUTE_TYPE_NUMBER,
            "range": models.ATTRIBUTE_TYPE_STRING,
            "indexed": models.ATTRIBUTE_TYPE_STRING,
            "str": models.ATTRIBUTE_TYPE_STRING,
            "numbr": models.ATTRIBUTE_TYPE_NUMBER,
            "blb": models.ATTRIBUTE_TYPE_BLOB,
            "set_number": models.ATTRIBUTE_TYPE_NUMBER_SET,
            "set_string": models.ATTRIBUTE_TYPE_STRING_SET,
            "set_blob": models.ATTRIBUTE_TYPE_BLOB_SET
        },
        ["id", "range"]
    )

    test_table_schema_with_index = models.TableSchema(
        {
            "id": models.ATTRIBUTE_TYPE_NUMBER,
            "range": models.ATTRIBUTE_TYPE_STRING,
            "indexed": models.ATTRIBUTE_TYPE_STRING,
            "str": models.ATTRIBUTE_TYPE_STRING,
            "numbr": models.ATTRIBUTE_TYPE_NUMBER,
            "blb": models.ATTRIBUTE_TYPE_BLOB,
            "set_number": models.ATTRIBUTE_TYPE_NUMBER_SET,
            "set_string": models.ATTRIBUTE_TYPE_STRING_SET,
            "set_blob": models.ATTRIBUTE_TYPE_BLOB_SET
        },
        ["id", "range"],
        {
            "index": models.IndexDefinition("indexed")
        }
    )

    test_data_predefined_fields = {
        'indexed': ('text', "'ind'", 'ind'),
        'str': ('text', "'str'", 'str'),
        'numbr': ('decimal', '1', 1),
        'blb': ('blob', '0x{}'.format(binascii.hexlify('blob')), 'blob'),
        'set_number': ('set<decimal>', '{1,2,3}', {1, 2, 3}),
        'set_string': ('set<text>', "{'a','b','c'}", {'a', 'b', 'c'}),
        'set_blob': (
            'set<blob>', '{{0x{}, 0x{}}}'.format(
                binascii.hexlify('blob1'), binascii.hexlify('blob2')),
            {'blob1', 'blob2'}
        )
    }

    test_data_system_fields = {
        SYSTEM_COLUMN_EXTRA_ATTR_DATA: 'map<text,blob>',
        SYSTEM_COLUMN_EXTRA_ATTR_TYPES: 'map<text,text>',
        SYSTEM_COLUMN_ATTR_EXIST: 'set<text>'
    }

    test_data_dynamic_fields = {
        'fnum': ('decimal', binascii.hexlify(json.dumps('1')), 1),
        'fstr': ('text', binascii.hexlify(json.dumps('fstr')), 'fstr'),
        'fblb': ('blob', binascii.hexlify(json.dumps('fblob')), 'fblob'),
        'fsnum': (
            'set<decimal>', binascii.hexlify(json.dumps(['1', '2', '3'])),
            {1, 2, 3}
        ),
        'fsstr': (
            'set<text>', binascii.hexlify(
                json.dumps(['fa', 'fb', 'fc'])), {'fa', 'fb', 'fc'}
        ),
        'fsblob': (
            'set<blob>', binascii.hexlify(json.dumps(['fblob1', 'fblob2'])),
            {'fblob1', 'fblob2'}
        )
    }

    C2S_TYPES = cassandra_impl.CASSANDRA_TO_STORAGE_TYPES

    @classmethod
    def setUpClass(cls):
        super(TestCassandraBase, cls).setUpClass()

        cls.CLUSTER = cluster.Cluster(**TEST_CONNECTION)
        cluster_hadler = ClusterHandler(cls.CLUSTER, query_timeout=300)
        table_info_repo = CassandraTableInfoRepository(cluster_hadler)
        storage_driver = cassandra_impl.CassandraStorageDriver(
            cluster_hadler, table_info_repo
        )
        cls.CASANDRA_STORAGE_IMPL = SimpleStorageManager(storage_driver,
                                                         table_info_repo)

        cls.SESSION = cls.CLUSTER.connect()
        cls.SESSION.row_factory = query.dict_factory
        cls.SESSION.default_timeout = 300

        if cls._tenant_scope == cls.TENANT_PER_TEST_CLASS:
            cls.tenant = cls._get_unique_name()
            cls._create_tenant(cls.tenant)

        cls.expected_data = {
            name: models.AttributeValue(cls.C2S_TYPES[typ], val)
            for name, (typ, _, val)
            in dict(cls.test_data_keys.items() +
                    cls.test_data_dynamic_fields.items() +
                    cls.test_data_predefined_fields.items()).iteritems()
        }

    @classmethod
    def tearDownClass(cls):
        super(TestCassandraBase, cls).tearDownClass()
        if cls._tenant_scope == cls.TENANT_PER_TEST_CLASS:
            cls._drop_tenant(cls.tenant)

    def setUp(self):
        if self._tenant_scope == self.TENANT_PER_TEST_METHOD:
            self.keyspace = self._get_unique_name()
            self._create_tenant(self.tenant)

        self.context = FakeContext(self.tenant)

        self.table_name = self._get_unique_name()

    def tearDown(self):
        if self._tenant_scope == self.TENANT_PER_TEST_METHOD:
            self._drop_tenant(self.tenant)

    @staticmethod
    def _get_unique_name():
        name = str(uuid.uuid4())
        return 'test' + filter(lambda x: x != '-', name)[:28]

    @classmethod
    def _create_tenant(cls, tenant):
        query = "CREATE KEYSPACE {}{} WITH replication".format(
            USER_PREFIX, tenant)
        query += " = {'class':'SimpleStrategy', 'replication_factor':1}"

        cls.SESSION.execute(query)

    @classmethod
    def _drop_tenant(cls, tenant):
        query = ("DROP KEYSPACE {}{}".format(USER_PREFIX, tenant))

        cls.SESSION.execute(query)

    def _get_table_names(self, tenant=None):

        query = (
            "SELECT name FROM magnetodb.table_info WHERE tenant='{}'"
        ).format(
            tenant or self.tenant
        )

        result = self.SESSION.execute(query)
        return [item["name"] for item in result]

    def _create_table(self, tenant=None, table_name=None, indexed=True):
        tenant = tenant or self.tenant
        table_name = table_name or self.table_name

        internal_table_name = USER_PREFIX + self._get_unique_name()
        keyspace = USER_PREFIX + tenant

        query = (
            "INSERT INTO magnetodb.table_info (tenant, name, exists, "
            '"schema", status, internal_name) '
            "VALUES('{}', '{}', 1, '{}', 'active', '{}') IF NOT EXISTS"
        ).format(
            tenant, table_name,
            self.test_table_schema_with_index.to_json() if indexed else
            self.test_table_schema.to_json(),
            internal_table_name
        )
        result = self.SESSION.execute(query)
        self.assertTrue(result[0]['[applied]'])

        query = "CREATE TABLE {}.{} (".format(keyspace, internal_table_name)

        for name, field in self.test_data_keys.iteritems():
            typ, _, _ = field
            query += '{}{} {},'.format(USER_PREFIX, name, typ)

        for name, field in self.test_data_predefined_fields.iteritems():
            typ, _, _ = field
            query += '{}{} {},'.format(USER_PREFIX, name, typ)

        for name, field in self.test_data_system_fields.iteritems():
            query += '{} {},'.format(name, field)

        if indexed:
            query += (
                "{} text, {} text, {} decimal, {} blob,"
                " PRIMARY KEY({}id, {}, {}, {}, {}, {}range))".format(
                    SYSTEM_COLUMN_INDEX_NAME,
                    SYSTEM_COLUMN_INDEX_VALUE_STRING,
                    SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                    SYSTEM_COLUMN_INDEX_VALUE_BLOB,
                    USER_PREFIX,
                    SYSTEM_COLUMN_INDEX_NAME,
                    SYSTEM_COLUMN_INDEX_VALUE_STRING,
                    SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                    SYSTEM_COLUMN_INDEX_VALUE_BLOB,
                    USER_PREFIX
                )
            )
        else:
            query += " PRIMARY KEY({}id, {}range))".format(
                USER_PREFIX, USER_PREFIX
            )
        self.SESSION.execute(query)

    def _drop_table(self, tenant=None, table_name=None):
        query = (
            "SELECT internal_name FROM magnetodb.table_info "
            "WHERE tenant='{}' and name='{}'"
        ).format(
            tenant, table_name
        )
        result = self.SESSION.execute(query)
        internal_table_name = result[0]["internal_name"]
        query = "DROP TABLE IF EXISTS {}{}.{}".format(
            USER_PREFIX, tenant, internal_table_name
        )
        self.SESSION.execute(query)
        query = (
            "DELETE FROM magnetodb.table_info "
            "WHERE tenant='{}' and name='{}'".format(
                tenant, table_name
            )
        )
        self.SESSION.execute(query)

    def _select_all(self, tenant=None, table_name=None):
        tenant = tenant or self.tenant
        table_name = table_name or self.table_name
        query = (
            'SELECT internal_name, "schema" FROM magnetodb.table_info '
            "WHERE tenant='{}' and name='{}'"
        ).format(
            tenant, table_name
        )
        result = self.SESSION.execute(query)
        internal_table_name = result[0]["internal_name"]
        schema = models.ModelBase.from_json(result[0]["schema"])

        query = "SELECT * FROM {}{}.{}".format(
            USER_PREFIX, tenant, internal_table_name)

        if schema.index_def_map:
            query += " WHERE {}={}".format(
                SYSTEM_COLUMN_INDEX_NAME,
                cassandra_impl.ENCODED_DEFAULT_STRING_VALUE
            )

        query += " ALLOW FILTERING"

        return self.SESSION.execute(query)

    def _insert_data(self, id_value=1, range_value='1',
                     predefined_fields=None, dynamic_fields=None):
        query = (
            'SELECT internal_name, "schema" FROM magnetodb.table_info '
            "WHERE tenant='{}' and name='{}'"
        ).format(
            self.tenant, self.table_name
        )

        result = self.SESSION.execute(query)
        internal_table_name = result[0]["internal_name"]
        schema = models.ModelBase.from_json(result[0]["schema"])

        query = "UPDATE {}{}.{} SET ".format(
            USER_PREFIX, self.tenant, internal_table_name)

        predefined_fields = (
            predefined_fields or self.test_data_predefined_fields
        )

        dynamic_fields = dynamic_fields or self.test_data_dynamic_fields

        set_items = []
        for name, field in predefined_fields.iteritems():
            _, sval, _ = field
            set_items.append('{}{}={}'.format(USER_PREFIX, name, sval))

        for name, field in dynamic_fields.iteritems():
            typ, sval, _ = field
            set_items.append("{}['{}'] = 0x{}".format(
                SYSTEM_COLUMN_EXTRA_ATTR_DATA, name, sval))
            set_items.append("{}['{}'] ='{}'".format(
                SYSTEM_COLUMN_EXTRA_ATTR_TYPES, name, typ))

        for name, field in dict(self.test_data_keys.items() +
                                predefined_fields.items() +
                                dynamic_fields.items()).iteritems():
            typ, sval, _ = field

            set_items.append("{} = {} + {{'{}'}}".format(
                SYSTEM_COLUMN_ATTR_EXIST, SYSTEM_COLUMN_ATTR_EXIST,
                name,
            ))

        query += ",".join(set_items)

        query += " WHERE {}id = {} AND {}range='{}'".format(
            USER_PREFIX, id_value, USER_PREFIX, range_value)

        if schema.index_def_map:
            default_index_cond_params = (
                SYSTEM_COLUMN_INDEX_NAME,
                cassandra_impl.ENCODED_DEFAULT_STRING_VALUE,
                SYSTEM_COLUMN_INDEX_VALUE_BLOB,
                cassandra_impl.ENCODED_DEFAULT_BLOB_VALUE,
                SYSTEM_COLUMN_INDEX_VALUE_STRING,
                cassandra_impl.ENCODED_DEFAULT_STRING_VALUE,
                SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                cassandra_impl.ENCODED_DEFAULT_NUMBER_VALUE
            )

            queries = [
                query + (
                    " AND {}={} AND {}={} AND {}={} AND {}={}".format(
                        *default_index_cond_params
                    )
                )
            ]

            for index_name, index_def in schema.index_def_map.iteritems():
                params = list(default_index_cond_params)
                params[1] = '{}'.format(index_name)
                index_type = schema.attribute_type_map[
                    index_def.attribute_to_index]

                index_value = predefined_fields[
                    index_def.attribute_to_index
                ][1]
                if index_type == models.ATTRIBUTE_TYPE_BLOB:
                    params[3] = index_value
                elif index_type == models.ATTRIBUTE_TYPE_STRING:
                    params[5] = index_value
                elif index_type == models.ATTRIBUTE_TYPE_NUMBER:
                    params[7] = index_value
                else:
                    self.fail()

                queries.append(
                    query +
                    " AND {}='{}' AND {}={} AND {}={} AND {}={}".format(
                        *params
                    )
                )

            query = (
                "BEGIN UNLOGGED BATCH " + " ".join(queries) + " APPLY BATCH"
            )

        self.SESSION.execute(query)

    def _validate_data(self, data):

        self.assertDictEqual(self.expected_data, data)


class TestCassandraTableCrud(TestCassandraBase):

    def test_create_table(self):
        self.assertEqual([], self._get_table_names())

        attrs = {}
        for name, (typ, _, _) in self.test_data_keys.iteritems():
            attrs[name] = self.C2S_TYPES[typ]
        for name, (typ, _, _) in self.test_data_predefined_fields.iteritems():
            attrs[name] = self.C2S_TYPES[typ]

        index_def_map = {
            'index_name': models.IndexDefinition('indexed')
        }

        schema = models.TableSchema(attrs, ['id', 'range'],
                                    index_def_map)

        self.CASANDRA_STORAGE_IMPL.create_table(
            self.context, self.table_name, schema
        )

        self.assertEqual([self.table_name], self._get_table_names())

    def test_list_table(self):
        self.assertNotIn(self.table_name,
                         self.CASANDRA_STORAGE_IMPL.list_tables(self.context))

        self._create_table()

        self.assertIn(self.table_name,
                      self.CASANDRA_STORAGE_IMPL.list_tables(self.context))

    def test_describe_table(self):

        self._create_table(indexed=True)

        attrs = {}
        for name, (typ, _, _) in self.test_data_keys.iteritems():
            attrs[name] = self.C2S_TYPES[typ]
        for name, (typ, _, _) in self.test_data_predefined_fields.iteritems():
            attrs[name] = self.C2S_TYPES[typ]

        index_def_map = {
            'index': models.IndexDefinition('indexed')
        }

        schema = models.TableSchema(attrs, ['id', 'range'],
                                    index_def_map)

        desc = self.CASANDRA_STORAGE_IMPL.describe_table(
            self.context, self.table_name)

        self.assertEqual(schema, desc.schema)

    def test_delete_table(self):
        self._create_table()

        self.assertIn(self.table_name, self._get_table_names())

        self.CASANDRA_STORAGE_IMPL.delete_table(self.context, self.table_name)

        self.assertNotIn(self.table_name, self._get_table_names())


class TestCassandraDeleteItem(TestCassandraBase):

    def test_delete_item_where(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value=2)

        del_req = models.DeleteItemRequest(
            self.table_name,
            {
                'id': models.AttributeValue.number(1),
                'range': models.AttributeValue.str('1')
            }
        )

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual('2', all[0]['user_range'])

    def test_delete_item_where_negative(self):
        self._create_table(indexed=True)
        self._insert_data()

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.AttributeValue.number(1),
             'range': models.AttributeValue.str('2')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]['user_id'])

    @unittest.skip("Skipped due to Cassandra 2.0.6 bug"
                   "(https://issues.apache.org/jira/browse/CASSANDRA-6914)")
    def test_delete_item_if_exists(self):
        self._create_table(indexed=True)
        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["user_id"])

        expected = {'str': [models.ExpectedCondition.exists()]}

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.AttributeValue.number(1),
             'range': models.AttributeValue.str('1')})

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req, expected
        )

        all = self._select_all()

        self.assertEqual(0, len(all))

    def test_delete_item_if_exists_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["user_id"])

        expected = {
            'not_existed_attr_name': [models.ExpectedCondition.exists()]
        }

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.AttributeValue.number(1),
             'range': models.AttributeValue.str('1')})

        self.assertRaises(exception.ConditionalCheckFailedException,
                          self.CASANDRA_STORAGE_IMPL.delete_item,
                          self.context, del_req, expected)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["user_id"])

    @unittest.skip("Skipped due to Cassandra 2.0.6 bug"
                   "(https://issues.apache.org/jira/browse/CASSANDRA-6914)")
    def test_delete_item_if_not_exists(self):
        self._create_table(indexed=True)

        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["user_id"])

        expected = {
            'not_existed_attr_name': [models.ExpectedCondition.not_exists()]
        }

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.AttributeValue.number(1),
             'range': models.AttributeValue.str('1')})

        result = self.CASANDRA_STORAGE_IMPL.delete_item(
            self.context, del_req, expected)

        self.assertTrue(result)

        all = self._select_all()

        self.assertEqual(0, len(all))

    def test_delete_item_if_not_exists_negative(self):
        self._create_table(indexed=True)
        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["user_id"])

        expected = {'str': [models.ExpectedCondition.not_exists()]}

        del_req = models.DeleteItemRequest(
            self.table_name,
            {'id': models.AttributeValue.number(1),
             'range': models.AttributeValue.str('1')})

        self.assertRaises(exception.ConditionalCheckFailedException,
                          self.CASANDRA_STORAGE_IMPL.delete_item,
                          self.context, del_req, expected)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["user_id"])


class TestCassandraSelectItem(TestCassandraBase):

    def test_select_item(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_no_condition(self):
        self._create_table(indexed=True)

        self._insert_data()

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_attr(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond,
            models.SelectType.specified_attributes(['fstr'])
        )

        self.assertEqual(1, result.count)
        self.assertEqual(
            {'fstr': models.AttributeValue(
                models.ATTRIBUTE_TYPE_STRING, 'fstr')},
            result.items[0])

    def test_select_item_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_item_less(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.lt(models.AttributeValue.str('2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_less_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.lt(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_item_less_eq(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.le(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_less_eq_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.le(models.AttributeValue.str('0'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_item_greater(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.gt(models.AttributeValue.str('0'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_greater_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.gt(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_item_greater_eq(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_greater_eq_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_item_indexed(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'indexed': [
                models.IndexedCondition.le(models.AttributeValue.str('ind'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond, index_name="index")

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_indexed_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'indexed': [
                models.IndexedCondition.lt(models.AttributeValue.str('ind'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond, index_name="index")

        self.assertEqual(0, result.count)

    def test_select_item_between(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='1')
        self._insert_data(range_value='3')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('1')),
                models.IndexedCondition.le(models.AttributeValue.str('2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_between2(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='-1')
        self._insert_data(range_value='1')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('0')),
                models.IndexedCondition.le(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_between_negative(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='1')
        self._insert_data(range_value='4')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('2')),
                models.IndexedCondition.le(models.AttributeValue.str('3'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_item_begins_with(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='1')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('1')),
                models.IndexedCondition.lt(models.AttributeValue.str(
                    '1'[:-1] + chr(ord('1'[-1]) + 1))
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_begins_with2(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='11')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('1')),
                models.IndexedCondition.lt(models.AttributeValue.str(
                    '1'[:-1] + chr(ord('1'[-1]) + 1))
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(1, result.count)
        self.assertIn('11', result.items[0]['range'].value)

    def test_select_item_begins_with_negative(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue.str('1')),
                models.IndexedCondition.lt(models.AttributeValue.str(
                    '1'[:-1] + chr(ord('1'[-1]) + 1))
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(0, result.count)

    def test_select_with_limit(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.ge(
                    models.AttributeValue.str('1')
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond)

        self.assertEqual(2, result.count)

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond, limit=1)

        self.assertEqual(1, result.count)

    def test_select_count(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond,
            models.SelectType.count())

        self.assertEqual(1, result.count)

    def test_select_item_exclusive_key(self):
        self._create_table(indexed=True)
        self._insert_data()

        exclusive_start_key = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('0')
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name,
            exclusive_start_key=exclusive_start_key)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_select_item_exclusive_key2(self):
        self._create_table(indexed=True)

        self._insert_data()
        self._insert_data(id_value=2)

        exclusive_start_key = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('0')
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, limit=1,
            exclusive_start_key=exclusive_start_key)

        exclusive_start_key2 = {
            'id': models.AttributeValue.number(2),
            'range': models.AttributeValue.str('0')
        }

        result2 = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, limit=1,
            exclusive_start_key=exclusive_start_key2)

        self.assertTrue(result.count == 1 or result2.count == 1)

    def test_select_item_exclusive_key_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        exclusive_start_key = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name,
            exclusive_start_key=exclusive_start_key)

        self.assertEqual(0, result.count)

    def test_select_item_exclusive_key_with_range(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='1')
        self._insert_data(range_value='2')
        self._insert_data(range_value='3')
        self._insert_data(range_value='4')
        self._insert_data(range_value='5')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [
                models.IndexedCondition.gt(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond,
            limit=2)

        self.assertEqual(2, result.count)
        self.assertEqual('2', result.items[0]['range'].value)
        self.assertEqual('3', result.items[1]['range'].value)

        last_eval_key = result.last_evaluated_key

        self.assertIsNotNone(last_eval_key)

        result2 = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, indexed_cond,
            exclusive_start_key=last_eval_key)

        self.assertEqual(2, result2.count)
        self.assertEqual('4', result2.items[0]['range'].value)
        self.assertEqual('5', result2.items[1]['range'].value)


class TestCassandraUpdateItem(TestCassandraBase):
    def test_update_item_put_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()
        expected['str'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING, 'new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'numbr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 42)),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['numbr'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_NUMBER, 42)

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'blb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_BLOB, 'new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['blb'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_BLOB, 'new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'set_string': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    models.ATTRIBUTE_TYPE_STRING_SET, {'new'})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['set_string'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING_SET, {'new'})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'set_number': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER_SET, {42})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['set_number'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_NUMBER_SET, {42})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'set_blob': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    models.ATTRIBUTE_TYPE_BLOB_SET, {'new'})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['set_blob'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_BLOB_SET, {'new'})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'fstr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fstr'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING, 'new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'fnum': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 42)),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fnum'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_NUMBER, 42)

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'fblb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(models.ATTRIBUTE_TYPE_BLOB, 'new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fblb'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_BLOB, 'new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'fsstr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    models.ATTRIBUTE_TYPE_STRING_SET, {'new1', 'new2'})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fsstr'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_STRING_SET, {'new1', 'new2'})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'fsnum': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    models.ATTRIBUTE_TYPE_NUMBER_SET, {42, 43})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fsnum'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_NUMBER_SET, {42, 43})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'fsblb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    models.ATTRIBUTE_TYPE_BLOB_SET, {'new1', 'new2'})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fsblb'] = models.AttributeValue(
            models.ATTRIBUTE_TYPE_BLOB_SET, {'new1', 'new2'})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)

    def test_update_item_delete(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1')
        }

        actions = {
            'str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE, None),
            'fstr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE, None)
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.context, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        del expected['str']
        del expected['fstr']

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, keys_condition)

        self.assertEqual([expected], result.items)


class TestCassandraPutItem(TestCassandraBase):
    def test_put_item_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'str': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'str'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1'),
            'number': models.AttributeValue.number(42),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('1'),
            'blb': models.AttributeValue.blob('blob'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_set_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'set_string': models.AttributeValue(
                models.ATTRIBUTE_TYPE_STRING_SET, {'str1', 'str2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_set_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'set_number': models.AttributeValue(
                models.ATTRIBUTE_TYPE_NUMBER_SET, {42, 43}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_set_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'set_blob': models.AttributeValue(
                models.ATTRIBUTE_TYPE_BLOB_SET, {'blob1', 'blob2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fstr': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'str')
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fnum': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 42),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fblb': models.AttributeValue(models.ATTRIBUTE_TYPE_BLOB, 'blob'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_set_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fsstr': models.AttributeValue(
                models.ATTRIBUTE_TYPE_STRING_SET, {'str1', 'str2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_set_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fsnum': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER_SET,
                                           {42, 43})
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_set_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fsblb': models.AttributeValue(
                models.ATTRIBUTE_TYPE_BLOB_SET, {'blob1', 'blob2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        self.CASANDRA_STORAGE_IMPL.put_item(self.context, put_request)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'str': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'str'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'str': [
                models.ExpectedCondition.eq(models.AttributeValue.str('str'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'numbr': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 42),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'numbr': [
                models.ExpectedCondition.eq(models.AttributeValue.number(1))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'blb': models.AttributeValue(models.ATTRIBUTE_TYPE_BLOB, 'blob'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'blb': [
                models.ExpectedCondition.eq(models.AttributeValue.blob('blob'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [
                models.Condition.eq(models.AttributeValue.number(1))
            ],
            'range': [
                models.Condition.eq(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'set_string': models.AttributeValue(
                models.ATTRIBUTE_TYPE_STRING_SET, {'str1', 'str2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'set_string': [
                models.ExpectedCondition.eq(models.AttributeValue.str_set(
                    {'a', 'b', 'c'}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'set_number': models.AttributeValue(
                models.ATTRIBUTE_TYPE_NUMBER_SET, {42, 43}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'set_number': [
                models.ExpectedCondition.eq(models.AttributeValue.number_set(
                    {1, 2, 3}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'set_blob': models.AttributeValue(
                models.ATTRIBUTE_TYPE_BLOB_SET, {'blob1', 'blob2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'set_blob': [
                models.ExpectedCondition.eq(models.AttributeValue.blob_set(
                    {'blob1', 'blob2'}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [
                models.Condition.eq(models.AttributeValue.number(1))
            ],
            'range': [
                models.Condition.eq(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fstr': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'str'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'fstr': [
                models.ExpectedCondition.eq(models.AttributeValue.str('fstr'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fnum': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 42),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'fnum': [
                models.ExpectedCondition.eq(models.AttributeValue.number(1))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [
                models.Condition.eq(models.AttributeValue.number(1))
            ],
            'range': [
                models.Condition.eq(models.AttributeValue.str('1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fblb': models.AttributeValue(models.ATTRIBUTE_TYPE_BLOB, 'blob'),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'fblb': [
                models.ExpectedCondition.eq(
                    models.AttributeValue.blob('fblob'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fsstr': models.AttributeValue(
                models.ATTRIBUTE_TYPE_STRING_SET, {'str1', 'str2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'fsstr': [
                models.ExpectedCondition.eq(
                    models.AttributeValue.str_set({'fa', 'fb', 'fc'}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fsnum': models.AttributeValue(
                models.ATTRIBUTE_TYPE_NUMBER_SET, {42, 43}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'fsnum': [
                models.ExpectedCondition.eq(
                    models.AttributeValue.number_set({1, 2, 3}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'fsblb': models.AttributeValue(
                models.ATTRIBUTE_TYPE_BLOB_SET, {'blob1', 'blob2'}),
        }

        put_request = models.PutItemRequest(self.table_name, put)

        expected = {
            'fsblob': [
                models.ExpectedCondition.eq(
                    models.AttributeValue.blob_set({'fblob1', 'fblob2'}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.context, put_request, expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.select_item(
            self.context, self.table_name, key_condition)

        self.assertEqual([put], result.items)


class TestCassandraScan(TestCassandraBase):

    def test_scan(self):
        self._create_table(indexed=True)
        self._insert_data()

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, {})

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_not_equal(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='2')

        condition = {
            'range': [models.ScanCondition.neq(models.AttributeValue.str('2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_contains(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='121')

        condition = {
            'range': [models.ScanCondition.contains(
                models.AttributeValue.str('2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(1, result.count)
        self.assertEqual('121', result.items[0]['range'].value)

    def test_scan_not_contains(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='22')

        condition = {
            'range': [models.ScanCondition.not_contains(
                models.AttributeValue.str('2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_contains_for_set(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.contains(
                models.AttributeValue.number(2))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_contains_for_set2(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.contains(
                models.AttributeValue.number(4))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(0, result.count)

    def test_scan_not_contains_for_set(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.not_contains(
                models.AttributeValue.number(4))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_not_contains_for_set2(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.not_contains(
                models.AttributeValue.number(2))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(0, result.count)

    def test_scan_in(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'range': [models.ScanCondition.in_set({
                models.AttributeValue.str('1'),
                models.AttributeValue.str('2')
            })]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_in_negative(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'range': [models.ScanCondition.in_set({
                models.AttributeValue.str('2'),
                models.AttributeValue.str('3')
            })]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, condition)

        self.assertEqual(0, result.count)

    def test_paging(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='2')
        self._insert_data(range_value='3')
        self._insert_data(id_value=2, range_value='1',)
        self._insert_data(id_value=2, range_value='2')

        last_evaluated_key = {
            'id': models.AttributeValue.number(1),
            'range': models.AttributeValue.str('2')
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, {},
            exclusive_start_key=last_evaluated_key,
            limit=2)

        last_evaluated_key2 = {
            'id': models.AttributeValue.number(2),
            'range': models.AttributeValue.str('1')
        }

        result2 = self.CASANDRA_STORAGE_IMPL.scan(
            self.context, self.table_name, {},
            exclusive_start_key=last_evaluated_key2,
            limit=2)

        if result.count == 2:
            self.assertTrue(result.items[0]['id'].value == 1 and
                            result.items[0]['range'].value == '3')

            self.assertTrue(result.items[1]['id'].value == 2 and
                            result.items[1]['range'].value == '1')

        elif result2.count == 2:
            self.assertTrue(result2.items[0]['id'].value == 2 and
                            result2.items[0]['range'].value == '2')

            self.assertTrue(result2.items[1]['id'].value == 1 and
                            result2.items[1]['range'].value == '1')

        else:
            self.fail()


class TestCassandraBatch(TestCassandraBase):
    def test_batch_write_item(self):
        self._create_table(indexed=True)
        put_items = [{
            'id': models.AttributeValue(models.ATTRIBUTE_TYPE_NUMBER, 1),
            'range': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, '1'),
            'str': models.AttributeValue(models.ATTRIBUTE_TYPE_STRING, 'str'),
        }, {
            'id': models.AttributeValue.number(2),
            'range': models.AttributeValue.str('2'),
            'number': models.AttributeValue.number(42),
        }, {
            'id': models.AttributeValue.number(3),
            'range': models.AttributeValue.str('3'),
            'blb': models.AttributeValue.blob('blob'),
        }]

        put_requests = [models.PutItemRequest(self.table_name, i)
                        for i in put_items]

        self.CASANDRA_STORAGE_IMPL.execute_write_batch(self.context,
                                                       put_requests)

        key_conditions = [{
            'id': [models.Condition.eq(models.AttributeValue.number(1))],
            'range': [models.Condition.eq(models.AttributeValue.str('1'))]
        }, {
            'id': [models.Condition.eq(models.AttributeValue.number(2))],
            'range': [models.Condition.eq(models.AttributeValue.str('2'))]
        }, {
            'id': [models.Condition.eq(models.AttributeValue.number(3))],
            'range': [models.Condition.eq(models.AttributeValue.str('3'))]
        }]

        for key, item in zip(key_conditions, put_items):
            result = self.CASANDRA_STORAGE_IMPL.select_item(
                self.context, self.table_name, key)

            self.assertEqual([item], result.items)
