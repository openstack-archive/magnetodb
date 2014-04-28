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
import unittest

import binascii
from magnetodb.storage import models
from magnetodb.storage.table_info_repo import TableInfo
from magnetodb.storage.driver.cassandra import query_builder


class CassandraQueryBuilderTestCase(unittest.TestCase):
    def test_generate_put_query(self):
        table_info = TableInfo(
            "table_name",
            models.TableSchema(
                {
                    "hash": models.ATTRIBUTE_TYPE_NUMBER,
                    "range": models.ATTRIBUTE_TYPE_STRING,
                },
                ["hash", "range"],
                None
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        table_info.internal_keyspace = "user_keyspace"
        table_info.internal_name = "user_table_name"

        attribute_map = {
            "hash": models.AttributeValue.number("1"),
            "range": models.AttributeValue.str("a"),
            "fbval": models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
        }

        query = query_builder.generate_put_query(
            table_info, attribute_map, old_indexes=None, if_not_exist=False,
            expected_condition_map=None
        )

        expected_query = (
            'INSERT INTO "user_keyspace"."user_table_name" ('
            '"user_range","user_hash",extra_attr_data,'
            'extra_attr_types,attr_exist) '
            'VALUES(\'a\',1,{\'fbval\':0x222f773d3d5c6e22},{\'fbval\':\'b\'},'
            '{\'fbval\',\'range\',\'hash\'})'
        )

        self.assertEqual(expected_query, query)

    def test_generate_put_query_if_not_exist(self):
        table_info = TableInfo(
            "table_name",
            models.TableSchema(
                {
                    "hash": models.ATTRIBUTE_TYPE_NUMBER,
                    "range": models.ATTRIBUTE_TYPE_STRING,
                },
                ["hash", "range"],
                None
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        table_info.internal_keyspace = "user_keyspace"
        table_info.internal_name = "user_table_name"

        attribute_map = {
            "hash": models.AttributeValue.number("1"),
            "range": models.AttributeValue.str("a"),
            "fbval": models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
        }

        query = query_builder.generate_put_query(
            table_info, attribute_map, old_indexes=None, if_not_exist=True,
            expected_condition_map=None
        )

        expected_query = (
            'INSERT INTO "user_keyspace"."user_table_name" ('
            '"user_range","user_hash",extra_attr_data,'
            'extra_attr_types,attr_exist) '
            'VALUES(\'a\',1,{\'fbval\':0x222f773d3d5c6e22},{\'fbval\':\'b\'},'
            '{\'fbval\',\'range\',\'hash\'}) IF NOT EXISTS'
        )

        self.assertEqual(expected_query, query)

    def test_generate_put_query_expected(self):
        table_info = TableInfo(
            "table_name",
            models.TableSchema(
                {
                    "hash": models.ATTRIBUTE_TYPE_NUMBER,
                    "range": models.ATTRIBUTE_TYPE_STRING,
                },
                ["hash", "range"],
                None
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        table_info.internal_keyspace = "user_keyspace"
        table_info.internal_name = "user_table_name"

        attribute_map = {
            "hash": models.AttributeValue.number("1"),
            "range": models.AttributeValue.str("a"),
            "fbval": models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
        }

        expected_condition_map = {
            "fbval": [
                models.ExpectedCondition.eq(
                    models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
                )
            ]
        }

        query = query_builder.generate_put_query(
            table_info, attribute_map, old_indexes=None, if_not_exist=False,
            expected_condition_map=expected_condition_map
        )

        expected_query = (
            'UPDATE "user_keyspace"."user_table_name" SET '
            'extra_attr_data={\'fbval\':0x222f773d3d5c6e22},'
            'extra_attr_types={\'fbval\':\'b\'},'
            'attr_exist={\'fbval\'} WHERE '
            '"user_hash"=1 AND "user_range"=\'a\' '
            'IF extra_attr_data[\'fbval\']=0x222f773d3d5c6e22'
        )

        self.assertEqual(expected_query, query)

    def test_generate_put_query_indexed_new(self):
        table_info = TableInfo(
            "table_name",
            models.TableSchema(
                {
                    "hash": models.ATTRIBUTE_TYPE_NUMBER,
                    "range": models.ATTRIBUTE_TYPE_STRING,
                    "indexed_value": models.ATTRIBUTE_TYPE_NUMBER,
                },
                ["hash", "range"],
                index_def_map={
                    "index_name": models.IndexDefinition("indexed_value")
                }
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        table_info.internal_keyspace = "user_keyspace"
        table_info.internal_name = "user_table_name"

        attribute_map = {
            "hash": models.AttributeValue.number("1"),
            "range": models.AttributeValue.str("a"),
            "indexed_value": models.AttributeValue.number("0"),
            "fbval": models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
        }

        query = query_builder.generate_put_query(
            table_info, attribute_map, old_indexes=None, if_not_exist=True,
            expected_condition_map=None
        )

        expected_query = (
            'BEGIN UNLOGGED BATCH INSERT '
            'INTO "user_keyspace"."user_table_name" ('
            'index_name,index_value_string,index_value_number,'
            'index_value_blob,"user_indexed_value","user_range","user_hash",'
            'extra_attr_data,extra_attr_types,attr_exist) VALUES('
            '\'\',\'\',0,0x,0,\'a\',1,{\'fbval\':0x222f773d3d5c6e22},'
            '{\'fbval\':\'b\'},{\'indexed_value\',\'fbval\',\'range\','
            '\'hash\'}) IF NOT EXISTS '
            'UPDATE "user_keyspace"."user_table_name" '
            'SET "user_indexed_value"=0,'
            'extra_attr_data={\'fbval\':0x222f773d3d5c6e22},'
            'extra_attr_types={\'fbval\':\'b\'},'
            'attr_exist={\'fbval\'} WHERE "user_hash"=1 AND '
            '"user_range"=\'a\' AND index_name=\'index_name\' '
            'AND index_value_string=\'\' AND index_value_number=0 '
            'AND index_value_blob=0x APPLY BATCH'
        )

        self.assertEqual(expected_query, query)

    def test_generate_put_query_indexed_old(self):
        table_info = TableInfo(
            "table_name",
            models.TableSchema(
                {
                    "hash": models.ATTRIBUTE_TYPE_NUMBER,
                    "range": models.ATTRIBUTE_TYPE_STRING,
                    "indexed_value": models.ATTRIBUTE_TYPE_NUMBER,
                },
                ["hash", "range"],
                index_def_map={
                    "index_name": models.IndexDefinition("indexed_value")
                }
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        table_info.internal_keyspace = "user_keyspace"
        table_info.internal_name = "user_table_name"

        attribute_map = {
            "hash": models.AttributeValue.number("1"),
            "range": models.AttributeValue.str("a"),
            "indexed_value": models.AttributeValue.number("0"),
            "fbval": models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
        }

        query = query_builder.generate_put_query(
            table_info, attribute_map, old_indexes={}, if_not_exist=False,
            expected_condition_map=None
        )

        expected_query = (
            'BEGIN UNLOGGED BATCH UPDATE "user_keyspace"."user_table_name" '
            'SET "user_indexed_value"=0,'
            'extra_attr_data={\'fbval\':0x222f773d3d5c6e22},'
            'extra_attr_types={\'fbval\':\'b\'},attr_exist={\'fbval\'} '
            'WHERE "user_hash"=1 AND "user_range"=\'a\' AND index_name=\'\' '
            'AND index_value_string=\'\' AND index_value_number=0 AND '
            'index_value_blob=0x IF "user_indexed_value"=null '
            'UPDATE "user_keyspace"."user_table_name" SET '
            '"user_indexed_value"=0,'
            'extra_attr_data={\'fbval\':0x222f773d3d5c6e22},'
            'extra_attr_types={\'fbval\':\'b\'},'
            'attr_exist={\'fbval\'} WHERE "user_hash"=1 AND '
            '"user_range"=\'a\' AND index_name=\'index_name\' AND '
            'index_value_string=\'\' AND index_value_number=0 AND '
            'index_value_blob=0x APPLY BATCH'
        )

        self.assertEqual(expected_query, query)

    def test_generate_put_query_indexed_old_expected(self):
        table_info = TableInfo(
            "table_name",
            models.TableSchema(
                {
                    "hash": models.ATTRIBUTE_TYPE_NUMBER,
                    "range": models.ATTRIBUTE_TYPE_STRING,
                    "indexed_value": models.ATTRIBUTE_TYPE_NUMBER,
                },
                ["hash", "range"],
                index_def_map={
                    "index_name": models.IndexDefinition("indexed_value")
                }
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        table_info.internal_keyspace = "user_keyspace"
        table_info.internal_name = "user_table_name"

        attribute_map = {
            "hash": models.AttributeValue.number("1"),
            "range": models.AttributeValue.str("a"),
            "indexed_value": models.AttributeValue.number("0"),
            "fbval": models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
        }

        expected_condition_map = {
            "fbval": [
                models.ExpectedCondition.eq(
                    models.AttributeValue.blob(binascii.b2a_base64("\xFF"))
                )
            ]
        }

        query = query_builder.generate_put_query(
            table_info, attribute_map, old_indexes=None, if_not_exist=False,
            expected_condition_map=expected_condition_map
        )

        expected_query = (
            'BEGIN UNLOGGED BATCH UPDATE "user_keyspace"."user_table_name" '
            'SET "user_indexed_value"=0,'
            'extra_attr_data={\'fbval\':0x222f773d3d5c6e22},'
            'extra_attr_types={\'fbval\':\'b\'},'
            'attr_exist={\'fbval\'} WHERE "user_hash"=1 AND '
            '"user_range"=\'a\' AND index_name=\'\' '
            'AND index_value_string=\'\' AND '
            'index_value_number=0 AND index_value_blob=0x IF '
            'extra_attr_data[\'fbval\']=0x222f773d3d5c6e22 '
            'UPDATE "user_keyspace"."user_table_name" SET '
            '"user_indexed_value"=0,'
            'extra_attr_data={\'fbval\':0x222f773d3d5c6e22},'
            'extra_attr_types={\'fbval\':\'b\'},attr_exist={\'fbval\'} WHERE '
            '"user_hash"=1 AND "user_range"=\'a\' AND '
            'index_name=\'index_name\' AND index_value_string=\'\' '
            'AND index_value_number=0 AND index_value_blob=0x APPLY BATCH'
        )

        self.assertEqual(expected_query, query)
