# Copyright 2014 Symantec Corporation
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

import copy

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest import test
from tempest_lib import exceptions


class MagnetoDBCreateTableNegativeTestCase(MagnetoDBTestCase):

    def __init__(self, *args, **kwargs):
        super(MagnetoDBCreateTableNegativeTestCase,
              self).__init__(*args, **kwargs)
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @test.attr(type=['CreT-49', 'negative'])
    def test_create_table_empty_table_name(self):
        tname = ''
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.schema_hash_only,
                                    self.smoke_lsi)

    @test.attr(type=['CreT-53_1', 'negative'])
    def test_create_table_index_without_name_param(self):
        lsi = copy.deepcopy(self.smoke_lsi)
        del lsi[0]['index_name']
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    lsi)

    @test.attr(type=['CreT-53_2', 'negative'])
    def test_create_table_index_without_schema_param(self):
        lsi = copy.deepcopy(self.smoke_lsi)
        del lsi[0]['key_schema']
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    lsi)

    @test.attr(type=['CreT-53_3', 'negative'])
    def test_create_table_index_without_projection_param(self):
        lsi = copy.deepcopy(self.smoke_lsi)
        del lsi[0]['projection']
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    lsi)

    @test.attr(type=['CreT-33', 'negative'])
    def test_create_table_invalid_schema_key_type(self):
        schema = [{'attribute_name': 'forum', 'key_type': 'INVALID'}]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.one_attr, self.tname, schema)

    @test.attr(type=['CreT-67', 'negative'])
    def test_create_table_2_same_index_name_in_one_table(self):
        index_attrs = [{'attribute_name': 'attr_name' + str(i),
                        'attribute_type': 'S'} for i in range(0, 2)]
        request_lsi = []
        for attribute in index_attrs:
            index_attr_name = attribute['attribute_name']
            request_lsi.append(
                {
                    'index_name': 'index_name',
                    'key_schema': [
                        {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                        {'attribute_name': index_attr_name,
                         'key_type': 'RANGE'}
                    ],
                    'projection': {'projection_type': 'ALL'}
                }
            )
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Two or more indexes with the same name", error_msg)

    @test.attr(type=['CreT-70', 'negative'])
    def test_create_table_index_hash_key_differs_from_table_hash(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       {'attribute_name': 'attr_name2',
                        'attribute_type': 'S'}
                       ]
        request_lsi = []
        request_lsi.append(
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': 'attr_name1', 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name2', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            }
        )
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-30', 'negative'])
    def test_create_table_empty_schema(self):
        attr_def = [{'attribute_name': 'forum', 'attribute_type': 'S'}]
        schema = []
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(attr_def=attr_def,
                                    table_name=self.tname,
                                    schema=schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("HASH key is missing", error_msg)

    @test.attr(type=['CreT-84_1', 'negative'])
    def test_create_table_index_non_key_attrs_all_projection_type(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        non_key_attributes = ['non_key_attr1']
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name1', 'key_type': 'RANGE'}
                ],
                'projection': {
                    'projection_type': 'ALL',
                    'non_key_attributes': non_key_attributes}
            }
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname, self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-84_2', 'negative'])
    def test_create_table_index_non_key_attrs_keys_only_projection_type(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        non_key_attributes = ['non_key_attr1']
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name1', 'key_type': 'RANGE'}
                ],
                'projection': {
                    'projection_type': 'KEYS_ONLY',
                    'non_key_attributes': non_key_attributes}
            }
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname, self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-71', 'negative'])
    def test_create_table_schema_hash_only_with_index(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name1', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            }
        ]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.one_attr + index_attrs,
                                    self.tname,
                                    self.schema_hash_only,
                                    request_lsi)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Table without range key in primary key schema "
                      "can not have indices",
                      error_msg)

    @test.attr(type=['CreT-72', 'negative'])
    def test_create_table_two_indexes_with_same_key(self):
        index_attrs = [{'attribute_name': 'attr_name',
                        'attribute_type': 'S'}]
        request_lsi = [
            {
                'index_name': 'index_name1',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            },
            {
                'index_name': 'index_name2',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            },
        ]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Table and its indices must have unique key schema",
                      error_msg)

    @test.attr(type=['CreT-73', 'negative'])
    def test_create_table_index_schema_only_hash(self):
        request_lsi = []
        request_lsi.append(
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                ],
                'projection': {'projection_type': 'ALL'}
            }
        )
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.smoke_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Range key in index wasn\'t specified", error_msg)

    @test.attr(type=['CreT-74', 'negative'])
    def test_create_table_index_schema_repeats_table_schema(self):
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': self.smoke_schema,
                'projection': {'projection_type': 'ALL'}
            }
        ]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.smoke_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Table and its indices must have unique key schema",
                      error_msg)

    @test.attr(type=['CreT-8_1', 'negative'])
    def test_create_table_malformed_01(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=None, table_name=self.tname,
                                    schema=None)

    @test.attr(type=['CreT-8_2', 'negative'])
    def test_create_table_malformed_02(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=self.smoke_attrs,
                                    table_name=self.tname,
                                    schema=None)

    @test.attr(type=['CreT-8_3', 'CreT-34', 'negative'])
    def test_create_table_malformed_03(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=None,
                                    table_name=self.tname,
                                    schema=self.smoke_schema)

    @test.attr(type=['CreT-8_4', 'negative'])
    def test_create_table_malformed_04(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=self.smoke_attrs,
                                    table_name=None,
                                    schema=self.smoke_schema)

    @test.attr(type=['CreT-8_5', 'negative'])
    def test_create_table_malformed_05(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=None, table_name=None,
                                    schema=self.smoke_schema)

    @test.attr(type=['CreT-8_6', 'negative'])
    def test_create_table_malformed_06(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=self.smoke_attrs,
                                    table_name=None,
                                    schema=None)

    @test.attr(type=['CreT-8_7', 'negative'])
    def test_create_table_malformed_07(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=None, table_name=None,
                                    schema=None)

    @test.attr(type=['CreT-20_2', 'negative'])
    def test_create_table_malformed_attrs_03(self):
        attr_def = [{'attribute_type': 'S'}]
        key_schema = [{'attribute_name': 'test', 'key_type': 'HASH'}]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(attr_def=attr_def,
                                    table_name=self.tname,
                                    schema=key_schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Required property \'attribute name\' wasn\'t found",
                      error_msg)

    @test.attr(type=['CreT-29', 'negative'])
    def test_create_table_only_range_key(self):
        attr_def = [{'attribute_name': 'forum', 'attribute_type': 'S'}]
        schema = [{'attribute_name': 'forum', 'key_type': 'RANGE'}]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(attr_def=attr_def,
                                    table_name=self.tname,
                                    schema=schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("HASH key is missing", error_msg)

    @test.attr(type=['CreT-35', 'negative'])
    def test_create_table_other_keys_in_schema(self):
        schema = [{'attribute_name': 'subject', 'key_type': 'HASH'}]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=self.one_attr,
                                    table_name=self.tname,
                                    schema=schema)

    @test.attr(type=['CreT-32_1', 'negative'])
    def test_create_table_redundand_schema_hash(self):
        schema = [{'attribute_name': 'last_posted_by', 'key_type': 'HASH'}]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema + schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Only one \'HASH\' key is allowed", error_msg)

    @test.attr(type=['CreT-32_2', 'negative'])
    def test_create_table_redundand_schema_range(self):
        schema = [{'attribute_name': 'last_posted_by', 'key_type': 'RANGE'}]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema + schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Only one 'RANGE' key is allowed", error_msg)

    @test.attr(type=['CreT-???', 'negative'])
    def test_create_table_no_attribute_type(self):
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=[
                {'attribute_name': self.hashkey},
                {'attribute_name': self.rangekey, 'attribute_type': 'S'}
            ], table_name=None, schema=None)

    @test.attr(type=['CreT-36_1', 'negative'])
    def test_create_table_hash_key_set(self):
        key_type = 'SS'
        attr_def = [{'attribute_name': 'set', 'attribute_type': key_type}]
        key_schema = [{'attribute_name': 'set', 'key_type': 'HASH'}]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(attr_def=attr_def,
                                    table_name=self.tname,
                                    schema=key_schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Type '%s' is not a scalar type" % key_type, error_msg)

    @test.attr(type=['CreT-36_2', 'negative'])
    def test_create_table_range_key_set(self):
        key_type = 'SS'
        attr_def = [{'attribute_name': 'set', 'attribute_type': key_type}]
        schema = [{'attribute_name': 'set', 'key_type': 'RANGE'}]
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self._create_test_table(attr_def=self.one_attr + attr_def,
                                    table_name=self.tname,
                                    schema=self.schema_hash_only + schema)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Type '%s' is not a scalar type" % key_type, error_msg)
