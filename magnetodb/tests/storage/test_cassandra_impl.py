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

import base64
import blist
from oslo_utils import timeutils
import decimal
import mock
import unittest
import uuid
import binascii

from cassandra import cluster
from cassandra import query
from cassandra import encoder
from oslo_serialization import jsonutils as json

from magnetodb import context as req_context

from magnetodb.common.cassandra import cluster_handler
from magnetodb.common import exception
from magnetodb.storage.driver.cassandra import cassandra_impl as driver
from magnetodb.storage.manager import simple_impl as manager
from magnetodb.storage import models
from magnetodb.storage.table_info_repo import (
    cassandra_impl as repo
)

TEST_CONNECTION = {
    'contact_points': ("localhost",),
    'control_connection_timeout': 60
}


class FakeContext(object):
    def __init__(self, tenant):
        self.tenant = tenant

    def to_dict(self):
        return {'tenant': self.tenant}


class TestCassandraBase(unittest.TestCase):
    TENANT_PER_TEST_METHOD = "test"
    TENANT_PER_TEST_CLASS = "class"

    _tenant_scope = TENANT_PER_TEST_CLASS

    test_data_keys = {
        'id': ('decimal', '1', 'N', 1),
        'range': ('text', "'1'", 'S', '1')
    }

    test_data_predefined_fields = {
        'indexed': ('text', "'ind'", 'S', 'ind'),
        'str': ('text', "'str'", 'S', 'str'),
        'numbr': ('decimal', '1', 'N', 1),
        'blb': ('blob', '0x{}'.format(binascii.hexlify('blob')), 'B', 'blob'),
        'set_number': ('set<decimal>', '{1,2,3}', 'NS',
                       blist.sortedset((1, 2, 3))),
        'set_string': ('set<text>', "{'a','b','c'}", 'SS',
                       blist.sortedset(('a', 'b', 'c'))),
        'set_blob': (
            'set<blob>', '{{0x{}, 0x{}}}'.format(
                binascii.hexlify('blob1'), binascii.hexlify('blob2')),
            'BS', blist.sortedset(('blob1', 'blob2'))
        ),
        'map_string_string': (
            'map<text,text>', "{'k1':'v1','k2':'v2'}", 'SSM',
            {'k1': 'v1', 'k2': 'v2'}
        ),
        'map_string_number': (
            'map<text,decimal>', "{'k1':1,'k2':2}", 'SNM',
            {'k1': 1, 'k2': 2}
        ),
        'map_string_blob': (
            'map<text,blob>',
            "{{'k1':0x{},'k2':0x{}}}".format(
                binascii.hexlify('blob1'), binascii.hexlify('blob2')
            ), 'SBM',
            {'k1': 'blob1', 'k2': 'blob2'}
        ),
        'map_number_string': (
            'map<decimal,text>', "{1:'v1',2:'v2'}", 'NSM',
            {1: 'v1', 2: 'v2'}
        ),
        'map_number_number': (
            'map<decimal,decimal>', "{1:1,2:2}", 'NNM',
            {1: 1, 2: 2}
        ),
        'map_number_blob': (
            'map<decimal,blob>',
            "{{1:0x{},2:0x{}}}".format(
                binascii.hexlify('blob1'), binascii.hexlify('blob2')
            ), 'NBM',
            {1: 'blob1', 2: 'blob2'}
        ),
        'map_blob_string': (
            'map<blob,text>',
            "{{0x{}:'v1',0x{}:'v2'}}".format(
                binascii.hexlify('blob1'), binascii.hexlify('blob2')
            ), 'BSM', {'blob1': 'v1', 'blob2': 'v2'}
        ),
        'map_blob_number': (
            'map<blob,decimal>',
            "{{0x{}:1,0x{}:2}}".format(
                binascii.hexlify('blob1'), binascii.hexlify('blob2')
            ), 'BNM', {'blob1': 1, 'blob2': 2}
        ),
        'map_blob_blob': (
            'map<blob,blob>',
            "{{0x{}:0x{},0x{}:0x{}}}".format(
                binascii.hexlify('blob1'), binascii.hexlify('blob1'),
                binascii.hexlify('blob2'), binascii.hexlify('blob2')
            ), 'BBM',
            {'blob1': 'blob1', 'blob2': 'blob2'}
        )
    }

    test_table_schema = models.TableSchema(
        {
            name: models.AttributeType(type)
            for name, (_, _, type, _) in
            test_data_keys.items() + test_data_predefined_fields.items()
        },
        ["id", "range"]
    )

    test_table_schema_with_index = models.TableSchema(
        test_table_schema.attribute_type_map,
        test_table_schema.key_attributes,
        {
            "index": models.IndexDefinition("id", "indexed")
        }
    )

    test_data_system_fields = {
        driver.SYSTEM_COLUMN_EXTRA_ATTR_DATA: 'map<text,blob>',
        driver.SYSTEM_COLUMN_EXTRA_ATTR_TYPES: 'map<text,text>',
        driver.SYSTEM_COLUMN_ATTR_EXIST: 'map<text,int>'
    }

    test_data_dynamic_fields = {
        'fnum': (None, binascii.hexlify(json.dumps('1')), 'N', 1),
        'fstr': (None, binascii.hexlify(json.dumps('fstr')), 'S', 'fstr'),
        'fblb': (
            None,
            binascii.hexlify(
                json.dumps(base64.b64encode('fblob'))
            ),
            'B',
            'fblob'
        ),
        'fsnum': (
            None, binascii.hexlify(json.dumps(['1', '2', '3'])),
            'NS', blist.sortedset((1, 2, 3))
        ),
        'fsstr': (
            None, binascii.hexlify(
                json.dumps(['fa', 'fb', 'fc'])), 'SS',
            blist.sortedset(('fa', 'fb', 'fc'))
        ),
        'fsblob': (
            None,
            binascii.hexlify(
                json.dumps([base64.b64encode('fblob1'),
                            base64.b64encode('fblob2')])
            ),
            'BS',
            blist.sortedset(('fblob1', 'fblob2'))
        ),
        'fm_str_str': (
            None,
            binascii.hexlify(
                json.dumps({'k1': 'v1', 'k2': 'v2'}, sort_keys=True)
            ),
            'SSM',
            {'k1': 'v1', 'k2': 'v2'}
        ),
        'fm_str_num': (
            None,
            binascii.hexlify(
                json.dumps({'k1': 1, 'k2': 2}, sort_keys=True)
            ),
            'SNM',
            {'k1': 1, 'k2': 2}
        ),
        'fm_str_blb': (
            None,
            binascii.hexlify(
                json.dumps(
                    {
                        'k1': base64.b64encode('fblob1'),
                        'k2': base64.b64encode('fblob2')
                    },
                    sort_keys=True
                )
            ),
            'SBM',
            {'k1': 'fblob1', 'k2': 'fblob2'}
        ),
        'fm_num_str': (
            None,
            binascii.hexlify(
                json.dumps({1: 'v1', 2: 'v2'}, sort_keys=True)
            ),
            'NSM',
            {1: 'v1', 2: 'v2'}
        ),
        'fm_num_num': (
            None,
            binascii.hexlify(
                json.dumps({1: 1, 2: 2}, sort_keys=True)
            ),
            'NNM',
            {1: 1, 2: 2}
        ),
        'fm_num_blb': (
            None,
            binascii.hexlify(
                json.dumps(
                    {
                        1: base64.b64encode('fblob1'),
                        2: base64.b64encode('fblob2')
                    },
                    sort_keys=True
                )
            ),
            'NBM',
            {1: 'fblob1', 2: 'fblob2'}
        ),
        'fm_blb_str': (
            None,
            binascii.hexlify(
                json.dumps(
                    {
                        base64.b64encode('fblob1'): 'v1',
                        base64.b64encode('fblob2'): 'v2'
                    }, sort_keys=True
                )
            ),
            'BSM',
            {'fblob1': 'v1', 'fblob2': 'v2'}
        ),
        'fm_blb_num': (
            None,
            binascii.hexlify(
                json.dumps(
                    {
                        base64.b64encode('fblob1'): 1,
                        base64.b64encode('fblob2'): 2
                    }, sort_keys=True
                )
            ),
            'BNM',
            {'fblob1': 1, 'fblob2': 2}
        ),
        'fm_blb_blb': (
            None,
            binascii.hexlify(
                json.dumps(
                    {
                        base64.b64encode('fblob1'): base64.b64encode('fblob1'),
                        base64.b64encode('fblob2'): base64.b64encode('fblob2')
                    }, sort_keys=True
                )
            ),
            'BBM',
            {"fblob1": 'fblob1', 'fblob2': 'fblob2'}
        )
    }

    @classmethod
    def setUpClass(cls):
        super(TestCassandraBase, cls).setUpClass()

        cls.notifier_patcher = mock.patch('magnetodb.notifier.get_notifier')
        cls.notifier_patcher.return_value = mock.Mock()
        cls.notifier_patcher.start()

        cls.CLUSTER = cluster.Cluster(**TEST_CONNECTION)
        cls.CLUSTER_HANDLER = cluster_handler.ClusterHandler(TEST_CONNECTION,
                                                             query_timeout=300)
        table_info_repo = repo.CassandraTableInfoRepository(
            cls.CLUSTER_HANDLER
        )

        default_keyspace_opts = {
            "replication": {
                "replication_factor": 1,
                "class": "SimpleStrategy"
            }
        }

        storage_driver = driver.CassandraStorageDriver(
            cls.CLUSTER_HANDLER, default_keyspace_opts
        )
        cls.CASANDRA_STORAGE_IMPL = manager.SimpleStorageManager(
            storage_driver, table_info_repo
        )

        cls.SESSION = cls.CLUSTER.connect()
        cls.SESSION.row_factory = query.dict_factory
        cls.SESSION.default_timeout = 300

        if cls._tenant_scope == cls.TENANT_PER_TEST_CLASS:
            cls.tenant = cls._get_unique_name()
            cls._create_tenant(cls.tenant)

        cls.expected_data = {
            name: models.AttributeValue(typ, decoded_value=val)
            for name, (_, _, typ, val)
            in dict(cls.test_data_keys.items() +
                    cls.test_data_dynamic_fields.items() +
                    cls.test_data_predefined_fields.items()).iteritems()
        }

    @classmethod
    def tearDownClass(cls):
        super(TestCassandraBase, cls).tearDownClass()
        cls.notifier_patcher.stop()
        if cls._tenant_scope == cls.TENANT_PER_TEST_CLASS:
            cls._drop_tenant(cls.tenant)
        cls.CLUSTER.shutdown()
        cls.CLUSTER_HANDLER.shutdown()

    def setUp(self):
        req_context.RequestContext()
        if self._tenant_scope == self.TENANT_PER_TEST_METHOD:
            self.keyspace = self._get_unique_name()
            self._create_tenant(self.tenant)

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
            driver.USER_PREFIX, tenant)
        query += " = {'class':'SimpleStrategy', 'replication_factor':1}"

        cls.SESSION.execute(query)

    @classmethod
    def _drop_tenant(cls, tenant):
        query = (
            "DROP KEYSPACE {}{}".format(driver.USER_PREFIX, tenant)
        )

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

        internal_table_name = '"{}{}"."{}{}"'.format(
            driver.USER_PREFIX, tenant, driver.USER_PREFIX,
            self._get_unique_name()
        )

        query = (
            "INSERT INTO magnetodb.table_info (tenant, name, id, exists, "
            '"schema", status, internal_name, last_update_date_time,'
            'creation_date_time) '
            "VALUES('{}', '{}', {}, 1, '{}', 'ACTIVE', '{}', {}, {})"
            " IF NOT EXISTS"
        ).format(
            tenant, table_name, '00000000-0000-0000-0000-000000000000',
            self.test_table_schema_with_index.to_json() if indexed else
            self.test_table_schema.to_json(),
            internal_table_name,
            encoder.Encoder().cql_encode_datetime(timeutils.utcnow()),
            encoder.Encoder().cql_encode_datetime(timeutils.utcnow())
        )
        result = self.SESSION.execute(query)
        self.assertTrue(result[0]['[applied]'])

        query = "CREATE TABLE {} (".format(internal_table_name)

        for name, field in self.test_data_keys.iteritems():
            typ, _, _, _ = field
            query += '{}{} {},'.format(driver.USER_PREFIX, name, typ)

        for name, field in self.test_data_predefined_fields.iteritems():
            typ, _, _, _ = field
            query += '{}{} {},'.format(driver.USER_PREFIX, name, typ)

        for name, field in self.test_data_system_fields.iteritems():
            query += '{} {},'.format(name, field)

        if indexed:
            query += (
                "{} text, {} text, {} decimal, {} blob,"
                " PRIMARY KEY({}id, {}, {}, {}, {}, {}range))".format(
                    driver.SYSTEM_COLUMN_INDEX_NAME,
                    driver.SYSTEM_COLUMN_INDEX_VALUE_STRING,
                    driver.SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                    driver.SYSTEM_COLUMN_INDEX_VALUE_BLOB,
                    driver.USER_PREFIX,
                    driver.SYSTEM_COLUMN_INDEX_NAME,
                    driver.SYSTEM_COLUMN_INDEX_VALUE_STRING,
                    driver.SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                    driver.SYSTEM_COLUMN_INDEX_VALUE_BLOB,
                    driver.USER_PREFIX
                )
            )
        else:
            query += " PRIMARY KEY({}id, {}range))".format(
                driver.USER_PREFIX, driver.USER_PREFIX
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
        query = "DROP TABLE IF EXISTS {}".format(internal_table_name)
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

        query = "SELECT * FROM {}".format(internal_table_name)

        if schema.index_def_map:
            query += " WHERE {}={}".format(
                driver.SYSTEM_COLUMN_INDEX_NAME,
                driver.ENCODED_DEFAULT_STRING_VALUE
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

        query = "UPDATE {} SET ".format(internal_table_name)

        predefined_fields = (
            predefined_fields or self.test_data_predefined_fields
        )

        dynamic_fields = dynamic_fields or self.test_data_dynamic_fields

        set_items = []
        for name, field in predefined_fields.iteritems():
            _, sval, _, _ = field
            set_items.append(
                '{}{}={}'.format(driver.USER_PREFIX, name, sval)
            )

        for name, field in dynamic_fields.iteritems():
            _, sval, typ, _ = field
            set_items.append("{}['{}'] = 0x{}".format(
                driver.SYSTEM_COLUMN_EXTRA_ATTR_DATA, name, sval)
            )
            set_items.append("{}['{}'] ='{}'".format(
                driver.SYSTEM_COLUMN_EXTRA_ATTR_TYPES, name, typ)
            )

        for name, field in dict(self.test_data_keys.items() +
                                predefined_fields.items() +
                                dynamic_fields.items()).iteritems():
            _, sval, typ, _ = field

            set_items.append(
                "{}['{}']=1".format(
                    driver.SYSTEM_COLUMN_ATTR_EXIST,
                    name
                )
            )

        query += ",".join(set_items)

        query += " WHERE {}id = {} AND {}range='{}'".format(
            driver.USER_PREFIX, id_value, driver.USER_PREFIX,
            range_value
        )

        if schema.index_def_map:
            default_index_cond_params = (
                driver.SYSTEM_COLUMN_INDEX_NAME,
                driver.ENCODED_DEFAULT_STRING_VALUE,
                driver.SYSTEM_COLUMN_INDEX_VALUE_BLOB,
                driver.ENCODED_DEFAULT_BLOB_VALUE,
                driver.SYSTEM_COLUMN_INDEX_VALUE_STRING,
                driver.ENCODED_DEFAULT_STRING_VALUE,
                driver.SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
                driver.ENCODED_DEFAULT_NUMBER_VALUE
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
                    index_def.alt_range_key_attr]

                index_value = predefined_fields[
                    index_def.alt_range_key_attr
                ][1]
                if index_type == models.AttributeType('B'):
                    params[3] = index_value
                elif index_type == models.AttributeType('S'):
                    params[5] = index_value
                elif index_type == models.AttributeType('N'):
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
        self.assertNotIn(self.table_name, self._get_table_names())

        attrs = {}
        for name, (_, _, typ, _) in self.test_data_keys.iteritems():
            attrs[name] = models.AttributeType(typ)
        for name, (_, _, typ, _) in (
                self.test_data_predefined_fields.iteritems()):
            attrs[name] = models.AttributeType(typ)

        index_def_map = {
            'index_name': models.IndexDefinition('id', 'indexed')
        }

        schema = models.TableSchema(attrs, ['id', 'range'],
                                    index_def_map)

        self.CASANDRA_STORAGE_IMPL.create_table(
            self.tenant, self.table_name, schema
        )

        self.assertIn(self.table_name, self._get_table_names())

    def test_create_duplicate_table(self):
        self.assertEqual([], self._get_table_names())

        attrs = {}
        for name, (_, _, typ, _) in self.test_data_keys.iteritems():
            attrs[name] = models.AttributeType(typ)
        for name, (_, _, typ, _) in (
                self.test_data_predefined_fields.iteritems()):
            attrs[name] = models.AttributeType(typ)

        index_def_map = {
            'index_name': models.IndexDefinition('id', 'indexed')
        }

        schema = models.TableSchema(attrs, ['id', 'range'],
                                    index_def_map)

        with self.assertRaises(exception.TableAlreadyExistsException):
            self.CASANDRA_STORAGE_IMPL.create_table(
                self.tenant, self.table_name, schema
            )

            self.assertEqual([self.table_name], self._get_table_names())

            self.CASANDRA_STORAGE_IMPL.create_table(
                self.tenant, self.table_name, schema
            )

    def test_list_table(self):
        self.assertNotIn(
            self.table_name,
            self.CASANDRA_STORAGE_IMPL.list_tables(
                self.tenant
            )
        )

        self._create_table()

        self.assertIn(
            self.table_name, self.CASANDRA_STORAGE_IMPL.list_tables(
                self.tenant
            )
        )

    def test_describe_table(self):

        self._create_table(indexed=True)

        attrs = {}
        for name, (_, _, typ, _) in self.test_data_keys.iteritems():
            attrs[name] = models.AttributeType(typ)
        for name, (_, _, typ, _) in (
                self.test_data_predefined_fields.iteritems()):
            attrs[name] = models.AttributeType(typ)

        index_def_map = {
            'index': models.IndexDefinition('id', 'indexed')
        }

        schema = models.TableSchema(attrs, ['id', 'range'],
                                    index_def_map)

        desc = self.CASANDRA_STORAGE_IMPL.describe_table(
            self.tenant, self.table_name)

        self.assertEqual(schema, desc.schema)

    def test_delete_table(self):
        self._create_table()

        self.assertIn(self.table_name, self._get_table_names())

        self.CASANDRA_STORAGE_IMPL.delete_table(self.tenant,
                                                self.table_name)

        self.assertNotIn(self.table_name, self._get_table_names())


class TestCassandraDeleteItem(TestCassandraBase):

    def test_delete_item_where(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value=2)

        del_item_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.tenant, self.table_name, del_item_key
        )

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual('2', all[0]['u_range'])

    def test_delete_item_where_negative(self):
        self._create_table(indexed=True)
        self._insert_data()

        del_item_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '2')
        }

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.tenant, self.table_name, del_item_key)

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]['u_id'])

    def test_delete_item_if_exists(self):
        self._create_table(indexed=True)
        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["u_id"])

        expected = {'str': [models.ExpectedCondition.not_null()]}

        del_item_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        self.CASANDRA_STORAGE_IMPL.delete_item(
            self.tenant, self.table_name, del_item_key, expected
        )

        all = self._select_all()

        self.assertEqual(0, len(all))

    def test_delete_item_if_exists_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["u_id"])

        expected = {
            'not_existed_attr_name': [models.ExpectedCondition.not_null()]
        }

        del_item_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        self.assertRaises(
            exception.ConditionalCheckFailedException,
            self.CASANDRA_STORAGE_IMPL.delete_item,
            self.tenant, self.table_name, del_item_key, expected
        )

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["u_id"])

    # @unittest.skip("Skipped due to Cassandra 2.0.6 bug"
    #                "(https://issues.apache.org/jira/browse/CASSANDRA-6914)")
    def test_delete_item_if_not_exists(self):
        self._create_table(indexed=True)

        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["u_id"])

        expected = {
            'not_existed_attr_name': [models.ExpectedCondition.null()]
        }

        del_item_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        result = self.CASANDRA_STORAGE_IMPL.delete_item(
            self.tenant, self.table_name, del_item_key, expected)

        self.assertTrue(result)

        all = self._select_all()

        self.assertEqual(0, len(all))

    def test_delete_item_if_not_exists_negative(self):
        self._create_table(indexed=True)
        self._insert_data()

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["u_id"])

        expected = {'str': [models.ExpectedCondition.null()]}

        del_item_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        self.assertRaises(
            exception.ConditionalCheckFailedException,
            self.CASANDRA_STORAGE_IMPL.delete_item,
            self.tenant, self.table_name, del_item_key, expected
        )

        all = self._select_all()

        self.assertEqual(1, len(all))
        self.assertEqual(1, all[0]["u_id"])


class TestCassandraSelectItem(TestCassandraBase):

    def test_query(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_attr(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.specific_attributes(['fstr'])
        )

        self.assertEqual(1, result.count)
        self.assertEqual(
            {'fstr': models.AttributeValue('S', 'fstr')},
            result.items[0])

    def test_query_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_query_less(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.lt(models.AttributeValue('S', '2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_less_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.lt(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_query_less_eq(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.le(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_less_eq_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.le(models.AttributeValue('S', '0'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_query_greater(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.gt(models.AttributeValue('S', '0'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_greater_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.gt(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_query_greater_eq(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_greater_eq_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_query_indexed(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'indexed': [
                models.IndexedCondition.le(models.AttributeValue('S', 'ind'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all(), index_name="index"
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_indexed_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'indexed': [
                models.IndexedCondition.lt(models.AttributeValue('S', 'ind'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all(), index_name="index"
        )

        self.assertEqual(0, result.count)

    def test_query_between(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='1')
        self._insert_data(range_value='3')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '1')),
                models.IndexedCondition.le(models.AttributeValue('S', '2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_between2(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='-1')
        self._insert_data(range_value='1')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '0')),
                models.IndexedCondition.le(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_between_negative(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='1')
        self._insert_data(range_value='4')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '2')),
                models.IndexedCondition.le(models.AttributeValue('S', '3'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_query_begins_with(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='1')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '1')),
                models.IndexedCondition.lt(
                    models.AttributeValue(
                        'S', '1'[:-1] + chr(ord('1'[-1]) + 1)
                    )
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_begins_with2(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='11')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '1')),
                models.IndexedCondition.lt(
                    models.AttributeValue(
                        'S', '1'[:-1] + chr(ord('1'[-1]) + 1)
                    )
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(1, result.count)
        self.assertIn('11', result.items[0]['range'].decoded_value)

    def test_query_begins_with_negative(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='0')
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(models.AttributeValue('S', '1')),
                models.IndexedCondition.lt(
                    models.AttributeValue(
                        'S', '1'[:-1] + chr(ord('1'[-1]) + 1)
                    )
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(0, result.count)

    def test_select_with_limit(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='2')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.ge(
                    models.AttributeValue('S', '1')
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all()
        )

        self.assertEqual(2, result.count)

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all(), limit=1
        )

        self.assertEqual(1, result.count)

    def test_select_count(self):
        self._create_table(indexed=True)

        self._insert_data()

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.count())

        self.assertEqual(1, result.count)

    def test_query_exclusive_key(self):
        self._create_table(indexed=True)
        self._insert_data()

        key_conditions = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
        }

        exclusive_start_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '0')
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_conditions,
            models.SelectType.all(),
            exclusive_start_key=exclusive_start_key
        )

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_query_exclusive_key2(self):
        self._create_table(indexed=True)

        self._insert_data()
        self._insert_data(id_value=2)

        key_conditions = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
        }

        exclusive_start_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '0')
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_conditions,
            models.SelectType.all(), limit=1,
            exclusive_start_key=exclusive_start_key
        )

        exclusive_start_key2 = {
            'id': models.AttributeValue('N', 2),
            'range': models.AttributeValue('S', '0')
        }

        result2 = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_conditions,
            models.SelectType.all(), limit=1,
            exclusive_start_key=exclusive_start_key2
        )

        self.assertTrue(result.count == 1 or result2.count == 1)

    def test_query_exclusive_key_negative(self):
        self._create_table(indexed=True)

        self._insert_data()

        key_conditions = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
        }

        exclusive_start_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_conditions,
            models.SelectType.all(), exclusive_start_key=exclusive_start_key
        )

        self.assertEqual(0, result.count)

    def test_query_exclusive_key_with_range(self):
        self._create_table(indexed=True)

        self._insert_data(range_value='1')
        self._insert_data(range_value='2')
        self._insert_data(range_value='3')
        self._insert_data(range_value='4')
        self._insert_data(range_value='5')

        indexed_cond = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [
                models.IndexedCondition.gt(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all(),
            limit=2
        )

        self.assertEqual(2, result.count)
        self.assertEqual('2', result.items[0]['range'].decoded_value)
        self.assertEqual('3', result.items[1]['range'].decoded_value)

        last_eval_key = result.last_evaluated_key

        self.assertIsNotNone(last_eval_key)

        result2 = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, indexed_cond,
            models.SelectType.all(), exclusive_start_key=last_eval_key
        )

        self.assertEqual(2, result2.count)
        self.assertEqual('4', result2.items[0]['range'].decoded_value)
        self.assertEqual('5', result2.items[1]['range'].decoded_value)


class TestCassandraUpdateItem(TestCassandraBase):
    def test_update_item_put_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('S', 'new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()
        expected['str'] = models.AttributeValue('S', 'new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'numbr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('N', 42)),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['numbr'] = models.AttributeValue('N', 42)

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'blb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('B', decoded_value='new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['blb'] = models.AttributeValue('B', decoded_value='new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'set_string': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SS', {'new'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['set_string'] = models.AttributeValue('SS', {'new'})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'set_number': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('NS', {42})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['set_number'] = models.AttributeValue('NS', {42})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'set_blob': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('BS',
                                      decoded_value=blist.sortedset(('new',)))
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['set_blob'] = models.AttributeValue(
            'BS', decoded_value=blist.sortedset(('new',))
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_str_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_string_string': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SSM', {'new1': 'new2'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_string_string'] = models.AttributeValue(
            'SSM', {'new1': 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_str_num(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_string_number': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SNM', {'new1': 1}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_string_number'] = models.AttributeValue(
            'SNM', {'new1': 1}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_str_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_string_blob': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SBM', decoded_value={'new1': 'blob1'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_string_blob'] = models.AttributeValue(
            'SBM', decoded_value={'new1': 'blob1'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_num_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_number_string': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('NSM', {1: 'new2'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_number_string'] = models.AttributeValue(
            'NSM', {1: 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_num_num(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_number_number': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('NNM', {213: "32.345"}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_number_number'] = models.AttributeValue(
            'NNM', {213: "32.345"}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_num_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_number_blob': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'NBM',
                    decoded_value={decimal.Decimal("324.54353"): 'blob1'}
                )
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_number_blob'] = models.AttributeValue(
            'NBM', decoded_value={decimal.Decimal("324.54353"): 'blob1'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_blob_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_blob_string': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('BSM', decoded_value={'blob1': 'new2'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_blob_string'] = models.AttributeValue(
            'BSM', decoded_value={'blob1': 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_blob_num(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_blob_number': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'BNM', decoded_value={'blob': decimal.Decimal("32.345")})
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_blob_number'] = models.AttributeValue(
            'BNM', decoded_value={'blob': decimal.Decimal("32.345")}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_map_blob_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'map_blob_blob': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'BBM', decoded_value={'blob2': 'blob1'}
                )
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['map_blob_blob'] = models.AttributeValue(
            'BBM', decoded_value={'blob2': 'blob1'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fstr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('S', 'new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fstr'] = models.AttributeValue('S', 'new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fnum': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('N', 42)),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fnum'] = models.AttributeValue('N', 42)

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fblb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('B', decoded_value='new')),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fblb'] = models.AttributeValue('B', decoded_value='new')

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fsstr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SS', {'new1', 'new2'})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fsstr'] = models.AttributeValue(
            'SS', {'new1', 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fsnum': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('NS', {42, 43})),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fsnum'] = models.AttributeValue('NS', {42, 43})

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fsblb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'BS', decoded_value=blist.sortedset(('new1', 'new2'))
                )
            ),
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fsblb'] = models.AttributeValue(
            'BS', decoded_value=blist.sortedset(('new1', 'new2'))
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_str_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_str_str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SSM', {'new1': 'new2'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_str_str'] = models.AttributeValue(
            'SSM', {'new1': 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_str_num(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_str_num': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SNM', {'new1': 1}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_str_num'] = models.AttributeValue(
            'SNM', {'new1': 1}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_str_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_str_blb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('SBM', decoded_value={'new1': 'blob1'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_str_blb'] = models.AttributeValue(
            'SBM', decoded_value={'new1': 'blob1'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_num_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_num_str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('NSM', {1: 'new2'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_num_str'] = models.AttributeValue(
            'NSM', {1: 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_num_num(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_num_num': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('NNM', {213: "32.345"}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_num_num'] = models.AttributeValue(
            'NNM', {213: "32.345"}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_num_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_num_blb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'NBM',
                    decoded_value={decimal.Decimal("324.54353"): 'blob1'}
                )
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_num_blb'] = models.AttributeValue(
            'NBM', decoded_value={decimal.Decimal("324.54353"): 'blob1'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_blob_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_blb_str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue('BSM', decoded_value={'blob1': 'new2'}))
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_blb_str'] = models.AttributeValue(
            'BSM', decoded_value={'blob1': 'new2'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_blob_num(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_blb_num': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'BNM', decoded_value={'blob': decimal.Decimal("32.345")})
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_blb_num'] = models.AttributeValue(
            'BNM', decoded_value={'blob': decimal.Decimal("32.345")}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_put_dynamic_map_blob_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'fm_blb_blb': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_PUT,
                models.AttributeValue(
                    'BBM', decoded_value={'blob2': 'blob1'}
                )
            )
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        expected['fm_blb_blb'] = models.AttributeValue(
            'BBM', decoded_value={'blob2': 'blob1'}
        )

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)

    def test_update_item_delete(self):
        self._create_table(indexed=True)
        self._insert_data()

        keys = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1')
        }

        actions = {
            'str': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE, None),
            'fstr': models.UpdateItemAction(
                models.UpdateItemAction.UPDATE_ACTION_DELETE, None)
        }

        self.CASANDRA_STORAGE_IMPL.update_item(
            self.tenant, self.table_name, keys, actions)

        expected = self.expected_data.copy()

        del expected['str']
        del expected['fstr']

        keys_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, keys_condition,
            models.SelectType.all()
        )

        self.assertEqual([expected], result.items)


class TestCassandraPutItem(TestCassandraBase):
    def test_put_item_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'str': models.AttributeValue('S', 'str'),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'number': models.AttributeValue('N', 42),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'blb': models.AttributeValue('B', decoded_value='blob'),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_set_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'set_string': models.AttributeValue('SS', {'str1', 'str2'})
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_set_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'set_number': models.AttributeValue('NS', {42, 43})
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_set_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'set_blob': models.AttributeValue(
                'BS', decoded_value=blist.sortedset(('blob1', 'blob2'))
            ),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_str_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_string_string': models.AttributeValue(
                'SSM', {'key123': 'value213'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_str_num(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_string_number': models.AttributeValue(
                'SNM', {'key123': 234}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_str_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_string_blob': models.AttributeValue(
                'SBM', decoded_value={'key123': 'blob1'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_num_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_number_string': models.AttributeValue(
                'NSM', {decimal.Decimal('568.42'): 'value213'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_num_num(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_number_number': models.AttributeValue(
                'NNM', {decimal.Decimal('345.234'): 234}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_num_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_number_blob': models.AttributeValue(
                'NBM', decoded_value={13: 'blob1'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_blob_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_blob_string': models.AttributeValue(
                'BSM', decoded_value={'\xFF\xFE': 'value213'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_blob_num(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_blob_number': models.AttributeValue(
                'BNM', decoded_value={'\xFF\xFF': 234}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_map_blob_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'map_blob_blob': models.AttributeValue(
                'BBM', decoded_value={'\xFE\xFF': 'blob1'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fstr': models.AttributeValue('S', 'str')
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fnum': models.AttributeValue('N', 42),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fblb': models.AttributeValue('B', decoded_value='blob'),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_set_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fsstr': models.AttributeValue('SS', {'str1', 'str2'}),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_set_number(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fsnum': models.AttributeValue('NS', {42, 43})
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_set_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fsblb': models.AttributeValue(
                'BS', decoded_value=blist.sortedset(('blob1', 'blob2'))
            ),
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_str_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_str_str': models.AttributeValue(
                'SSM', {'key123': 'value213'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_str_num(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_str_num': models.AttributeValue(
                'SNM', {'key123': 234}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_str_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_str_blb': models.AttributeValue(
                'SBM', decoded_value={'key123': 'blob1'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_num_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_num_str': models.AttributeValue(
                'NSM', {decimal.Decimal('568.42'): 'value213'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_num_num(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_num_num': models.AttributeValue(
                'NNM', {decimal.Decimal('345.234'): 234}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_num_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_num_blb': models.AttributeValue(
                'NBM', decoded_value={13: 'blob1'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_blob_str(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_blb_str': models.AttributeValue(
                'BSM', decoded_value={'\xFF\xFE': 'value213'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_blob_num(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_blb_num': models.AttributeValue(
                'BNM', decoded_value={'\xFF\xFF': 234}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant, self.table_name,
                                            put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_dynamic_map_blob_blob(self):
        self._create_table(indexed=True)

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fm_blb_blb': models.AttributeValue(
                'BBM', decoded_value={'\xFE\xFF': 'blob1'}
            )
        }

        self.CASANDRA_STORAGE_IMPL.put_item(self.tenant,
                                            self.table_name, put)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'str': models.AttributeValue('S', 'str'),
        }

        expected = {
            'str': [
                models.ExpectedCondition.eq(models.AttributeValue('S', 'str'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'numbr': models.AttributeValue('N', 42),
        }

        expected = {
            'numbr': [
                models.ExpectedCondition.eq(models.AttributeValue('N', 1))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'blb': models.AttributeValue('B', decoded_value='blob'),
        }

        expected = {
            'blb': [
                models.ExpectedCondition.eq(
                    models.AttributeValue('B', decoded_value='blob')
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [
                models.Condition.eq(models.AttributeValue('N', 1))
            ],
            'range': [
                models.Condition.eq(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'set_string': models.AttributeValue('SS', {'str1', 'str2'})
        }

        expected = {
            'set_string': [
                models.ExpectedCondition.eq(
                    models.AttributeValue('SS', {'a', 'b', 'c'})
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'set_number': models.AttributeValue('NS', {42, 43}),
        }

        expected = {
            'set_number': [
                models.ExpectedCondition.eq(
                    models.AttributeValue('NS', {1, 2, 3})
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'set_blob': models.AttributeValue(
                'BS', decoded_value=blist.sortedset(('blob1', 'blob2'))
            ),
        }

        expected = {
            'set_blob': [
                models.ExpectedCondition.eq(
                    models.AttributeValue(
                        'BS', decoded_value=blist.sortedset(('blob1', 'blob2'))
                    )
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [
                models.Condition.eq(models.AttributeValue('N', 1))
            ],
            'range': [
                models.Condition.eq(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fstr': models.AttributeValue('S', 'str'),
        }

        expected = {
            'fstr': [
                models.ExpectedCondition.eq(models.AttributeValue('S', 'fstr'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fnum': models.AttributeValue('N', 42),
        }

        expected = {
            'fnum': [
                models.ExpectedCondition.eq(models.AttributeValue('N', 1))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [
                models.Condition.eq(models.AttributeValue('N', 1))
            ],
            'range': [
                models.Condition.eq(models.AttributeValue('S', '1'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fblb': models.AttributeValue('B', decoded_value='blob'),
        }

        expected = {
            'fblb': [
                models.ExpectedCondition.eq(
                    models.AttributeValue('B', decoded_value='fblob'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_set_str(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fsstr': models.AttributeValue('SS', {'str1', 'str2'})
        }

        expected = {
            'fsstr': [
                models.ExpectedCondition.eq(
                    models.AttributeValue('SS', {'fa', 'fb', 'fc'})
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_set_number(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fsnum': models.AttributeValue('NS', {42, 43}),
        }

        expected = {
            'fsnum': [
                models.ExpectedCondition.eq(
                    models.AttributeValue('NS', {1, 2, 3}))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)

    def test_put_item_expected_dynamic_set_blob(self):
        self._create_table(indexed=True)
        self._insert_data()

        put = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'fsblb': models.AttributeValue(
                'BS', decoded_value=blist.sortedset(('blob1', 'blob2'))
            ),
        }

        expected = {
            'fsblob': [
                models.ExpectedCondition.eq(
                    models.AttributeValue(
                        'BS',
                        decoded_value=blist.sortedset(('fblob1', 'fblob2'))
                    )
                )
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.put_item(
            self.tenant, self.table_name, put,
            expected_condition_map=expected
        )
        self.assertTrue(result)

        key_condition = {
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.query(
            self.tenant, self.table_name, key_condition,
            models.SelectType.all()
        )

        self.assertEqual([put], result.items)


class TestCassandraScan(TestCassandraBase):

    def test_scan(self):
        self._create_table(indexed=True)
        self._insert_data()

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, {})

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_not_equal(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='2')

        condition = {
            'range': [
                models.ScanCondition.neq(models.AttributeValue('S', '2'))
            ]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_contains(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='121')

        condition = {
            'range': [models.ScanCondition.contains(
                models.AttributeValue('S', '2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(1, result.count)
        self.assertEqual('121', result.items[0]['range'].decoded_value)

    def test_scan_not_contains(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='22')

        condition = {
            'range': [models.ScanCondition.not_contains(
                models.AttributeValue('S', '2'))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_contains_for_set(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.contains(
                models.AttributeValue('N', 2))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_contains_for_set2(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.contains(
                models.AttributeValue('N', 4))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(0, result.count)

    def test_scan_not_contains_for_set(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.not_contains(
                models.AttributeValue('N', 4))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_not_contains_for_set2(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'set_number': [models.ScanCondition.not_contains(
                models.AttributeValue('N', 2))]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(0, result.count)

    def test_scan_in(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'range': [models.ScanCondition.in_set({
                models.AttributeValue('S', '1'),
                models.AttributeValue('S', '2')
            })]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(1, result.count)
        self._validate_data(result.items[0])

    def test_scan_in_negative(self):
        self._create_table(indexed=True)
        self._insert_data()

        condition = {
            'range': [models.ScanCondition.in_set({
                models.AttributeValue('S', '2'),
                models.AttributeValue('S', '3')
            })]
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, condition)

        self.assertEqual(0, result.count)

    def test_paging(self):
        self._create_table(indexed=True)
        self._insert_data()
        self._insert_data(range_value='2')
        self._insert_data(range_value='3')
        self._insert_data(id_value=2, range_value='1',)
        self._insert_data(id_value=2, range_value='2')

        last_evaluated_key = {
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '2')
        }

        result = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, {},
            exclusive_start_key=last_evaluated_key,
            limit=2)

        last_evaluated_key2 = {
            'id': models.AttributeValue('N', 2),
            'range': models.AttributeValue('S', '1')
        }

        result2 = self.CASANDRA_STORAGE_IMPL.scan(
            self.tenant, self.table_name, {},
            exclusive_start_key=last_evaluated_key2,
            limit=2)

        if result.count == 2:
            self.assertTrue(result.items[0]['id'].decoded_value == 1 and
                            result.items[0]['range'].decoded_value == '3')

            self.assertTrue(result.items[1]['id'].decoded_value == 2 and
                            result.items[1]['range'].decoded_value == '1')

        elif result2.count == 2:
            self.assertTrue(result2.items[0]['id'].decoded_value == 2 and
                            result2.items[0]['range'].decoded_value == '2')

            self.assertTrue(result2.items[1]['id'].decoded_value == 1 and
                            result2.items[1]['range'].decoded_value == '1')

        else:
            self.fail()


class TestCassandraBatch(TestCassandraBase):
    def test_batch_write_item(self):
        self._create_table(indexed=True)
        put_items = [{
            'id': models.AttributeValue('N', 1),
            'range': models.AttributeValue('S', '1'),
            'str': models.AttributeValue('S', 'str'),
        }, {
            'id': models.AttributeValue('N', 2),
            'range': models.AttributeValue('S', '2'),
            'number': models.AttributeValue('N', 42),
        }, {
            'id': models.AttributeValue('N', 3),
            'range': models.AttributeValue('S', '3'),
            'blb': models.AttributeValue('B', decoded_value='blob'),
        }]

        put_requests = {
            self.table_name: [
                models.WriteItemRequest.put(item) for item in put_items
            ]
        }

        self.CASANDRA_STORAGE_IMPL.execute_write_batch(self.tenant,
                                                       put_requests)

        key_conditions = [{
            'id': [models.Condition.eq(models.AttributeValue('N', 1))],
            'range': [models.Condition.eq(models.AttributeValue('S', '1'))]
        }, {
            'id': [models.Condition.eq(models.AttributeValue('N', 2))],
            'range': [models.Condition.eq(models.AttributeValue('S', '2'))]
        }, {
            'id': [models.Condition.eq(models.AttributeValue('N', 3))],
            'range': [models.Condition.eq(models.AttributeValue('S', '3'))]
        }]

        for key, item in zip(key_conditions, put_items):
            result = self.CASANDRA_STORAGE_IMPL.query(
                self.tenant, self.table_name, key,
                models.SelectType.all()
            )

            self.assertEqual([item], result.items)
