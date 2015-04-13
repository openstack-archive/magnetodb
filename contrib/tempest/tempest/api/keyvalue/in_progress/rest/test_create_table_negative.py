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

from tempest_lib import exceptions

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest import test


class MagnetoDBCreateTableNegativeTestCase(MagnetoDBTestCase):

    def __init__(self, *args, **kwargs):
        super(MagnetoDBCreateTableNegativeTestCase,
              self).__init__(*args, **kwargs)
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @test.attr(type=['CreT-3', 'CreT-101', 'negative'])
    def test_duplicate_table(self):
        self._create_test_table(self.smoke_attrs, self.tname,
                                self.smoke_schema)
        with self.assertRaises(exceptions.Duplicate):
            self._create_test_table(self.smoke_attrs, self.tname,
                                    self.smoke_schema)

    @test.attr(type=['CreT-20_1', 'negative'])
    def test_create_table_malformed_attrs_02(self):
        attr_def = [{'attribute_name': 'test'}]
        key_schema = [{'attribute_name': 'test', 'key_type': 'HASH'}]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def=attr_def,
                                    table_name=self.tname,
                                    schema=key_schema)

    @test.attr(type=['CreT-46', 'negative'])
    def test_create_table_too_short_name(self):
        tname = 'qq'
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.smoke_schema,
                                    self.smoke_lsi,
                                    wait_for_active=True)

    @test.attr(type=['CreT-47', 'negative'])
    def test_create_table_too_long_name(self):
        tname = self.tname + 'q' * (256 - len(self.tname))
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.smoke_schema,
                                    self.smoke_lsi)

    @test.attr(type=['CreT-18', 'negative'])
    def test_create_table_too_long_attr_names(self):
        attr_def = [
            {'attribute_name': 'f' * 256, 'attribute_type': 'S'},
            {'attribute_name': 's' * 256, 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'f' * 256, 'key_type': 'HASH'},
            {'attribute_name': 's' * 256, 'key_type': 'RANGE'}
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(attr_def, self.tname, schema)

    @test.attr(type=['CreT-56', 'negative'])
    def test_create_table_six_indexes(self):
        index_attrs = [{'attribute_name': 'attr_name' + str(i),
                        'attribute_type': 'S'} for i in range(0, 6)]
        request_lsi = []
        for attribute in index_attrs:
            index_attr_name = attribute['attribute_name']
            request_lsi.append(
                {
                    'index_name': index_attr_name + '_index',
                    'key_schema': [
                        {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                        {'attribute_name': index_attr_name,
                         'key_type': 'RANGE'}
                    ],
                    'projection': {'projection_type': 'ALL'}
                }
            )
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-57', 'negative'])
    def test_create_table_empty_indexes(self):
        request_lsi = []
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-63', 'negative'])
    def test_create_table_index_name_2_char(self):
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'qq'
        request_lsi[0]['index_name'] = request_index_name
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-64', 'negative'])
    def test_create_table_index_name_256_char(self):
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'q' * 256
        request_lsi[0]['index_name'] = request_index_name
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-65', 'negative'])
    def test_create_table_index_name_upper_case(self):
        # TODO(aostapenko) clarify behavior on dynamodb
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'INDEX_NAME'
        request_lsi[0]['index_name'] = request_index_name
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-80', 'negative'])
    def test_create_table_index_without_projection_type_with_non_key(self):
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
                'projection': {'non_key_attributes': ['attr1', 'attr2']}
            }
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-82', 'negative'])
    def test_create_table_index_key_attr_in_non_key(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        non_key_attributes = ['attr_name1', self.hashkey, self.rangekey]
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name1', 'key_type': 'RANGE'}
                ],
                'projection': {
                    'projection_type': 'INCLUDE',
                    'non_key_attributes': non_key_attributes}
            }
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-83', 'negative'])
    def test_create_table_index_non_key_empty(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        non_key_attributes = []
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name1', 'key_type': 'RANGE'}
                ],
                'projection': {
                    'projection_type': 'INCLUDE',
                    'non_key_attributes': non_key_attributes}
            }
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-86', 'negative'])
    def test_create_table_index_21_non_key(self):
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        non_key_attributes = ['non_key_attr' + i for i in range(0, 21)]
        request_lsi = [
            {
                'index_name': 'index_name',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'attr_name1', 'key_type': 'RANGE'}
                ],
                'projection': {
                    'projection_type': 'INCLUDE',
                    'non_key_attributes': non_key_attributes}
            }
        ]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname, self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-88', 'negative'])
    def test_create_table_index_21_non_key_3_indexes(self):
        index_attrs = [{'attribute_name': 'attr_name' + str(i),
                        'attribute_type': 'S'} for i in range(1, 4)
                       ]
        non_key_attributes = (['non_key_attr' + str(i) for i in range(1, 8)],
                              ['non_key_attr' + str(i) for i in range(8, 15)],
                              ['non_key_attr' + str(i) for i in range(15, 22)])
        request_lsi = []
        for i, attr_name in enumerate(index_attrs):
            request_lsi.append(
                {
                    'index_name': attr_name['attribute_name'] + 'index',
                    'key_schema': [
                        {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                        {'attribute_name': attr_name['attribute_name'],
                         'key_type': 'RANGE'}
                    ],
                    'projection': {
                        'projection_type': 'INCLUDE',
                        'non_key_attributes': non_key_attributes[i]}
                }
            )
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname, self.smoke_schema,
                                    request_lsi)

    @test.attr(type=['CreT-89', 'negative'])
    def test_create_table_index_21_non_key_3_indexes_repetitions(self):
        index_attrs = [{'attribute_name': 'attr_name' + str(i),
                        'attribute_type': 'S'} for i in range(1, 4)
                       ]
        non_key_attributes = (['non_key_attr' + str(i) for i in range(1, 8)],
                              ['non_key_attr' + str(i) for i in range(5, 12)],
                              ['non_key_attr' + str(i) for i in range(10, 16)])
        request_lsi = []
        for i, attr_name in enumerate(index_attrs):
            request_lsi.append(
                {
                    'index_name': attr_name['attribute_name'] + 'index',
                    'key_schema': [
                        {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                        {'attribute_name': attr_name['attribute_name'],
                         'key_type': 'RANGE'}
                    ],
                    'projection': {
                        'projection_type': 'INCLUDE',
                        'non_key_attributes': non_key_attributes[i]}
                }
            )
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + index_attrs,
                                    self.tname, self.smoke_schema,
                                    request_lsi)
#
#    @test.attr(type=['CreT-103', 'negative'])
#    def test_create_table_more_than_255(self):
#        for i in range(0, 255):
#            tname = rand_name(self.table_prefix).replace('-', '')
#            self._create_test_table(self.smoke_attrs, tname,
#                                    self.smoke_schema)
#        with self.assertRaises(exceptions.BadRequest):
#            self._create_test_table(self.smoke_attrs, self.tname,
#                                    self.smoke_schema)
