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
from tempest.test import attr


class MagnetoDBCreateTableTestCase(MagnetoDBTestCase):

    def _verify_table_response(self, method, response,
                               attributes, table_name,
                               key_schema,
                               local_secondary_indexes=None):

        self.assertTrue('table_description' in response)
        content = response['table_description']
        self.assertEqual(table_name, content['table_name'])
        self.assertIn('table_id', content)
        self.assertEqual(len(attributes),
                         len(content['attribute_definitions']))
        for attribute in content['attribute_definitions']:
            self.assertIn(attribute, attributes)
        self.assertEqual(0, content['item_count'])
        self.assertEqual(key_schema, content['key_schema'])
        self.assertEqual(0, content['table_size_bytes'])

        if local_secondary_indexes is None:
            self.assertNotIn('local_secondary_indexes', content)
        else:
            expected = list(local_secondary_indexes)
            for index in expected:
                index['index_size_bytes'] = 0
                index['item_count'] = 0
            self.assertEqual(expected, content['local_secondary_indexes'])

        self.assertIn('creation_date_time', content)

        self.assertIn(content['table_status'], ['CREATING', 'ACTIVE'])

    @attr(type=['CreT-1'])
    def test_create_table(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, self.smoke_attrs,
                                    tname, self.smoke_schema)
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-44'])
    def test_create_table_min_table_name(self):
        tname = 'qqq'
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(tname, body['table_description']['table_name'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-2', 'Cret-93', 'Cret-94', 'Cret-95', 'Cret-97'])
    def test_create_table_all_params(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table',
                                    body,
                                    self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.smoke_schema,
                                    request_lsi)
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-17_1'])
    def test_create_table_min_attr_length(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'f', 'attribute_type': 'S'},
            {'attribute_name': 's', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'f', 'key_type': 'HASH'},
            {'attribute_name': 's', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        desc = body['table_description']
        self.assertEqual(len(attr_def), len(desc['attribute_definitions']))
        for attribute in attr_def:
            self.assertIn(attribute, desc['attribute_definitions'])
        self.assertEqual(schema, desc['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-17_2'])
    def test_create_table_max_attr_length(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'f' * 255, 'attribute_type': 'S'},
            {'attribute_name': 's' * 255, 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'f' * 255, 'key_type': 'HASH'},
            {'attribute_name': 's' * 255, 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        desc = body['table_description']
        self.assertEqual(len(attr_def), len(desc['attribute_definitions']))
        for attribute in attr_def:
            self.assertIn(attribute, desc['attribute_definitions'])
        self.assertEqual(schema, desc['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-21_1'])
    def test_create_table_s(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'S'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        desc = body['table_description']
        self.assertEqual(len(attr_def), len(desc['attribute_definitions']))
        for attribute in attr_def:
            self.assertIn(attribute, desc['attribute_definitions'])
        self.assertEqual(schema, desc['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-21_2'])
    def test_create_table_n(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        desc = body['table_description']
        self.assertEqual(len(attr_def), len(desc['attribute_definitions']))
        for attribute in attr_def:
            self.assertIn(attribute, desc['attribute_definitions'])
        self.assertEqual(schema, desc['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-28'])
    def test_create_table_hash_range(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        desc = body['table_description']
        self.assertEqual(len(attr_def), len(desc['attribute_definitions']))
        for attribute in attr_def:
            self.assertIn(attribute, desc['attribute_definitions'])
        self.assertEqual(schema, desc['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-29'])
    def test_create_table_wrong_order_schema(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'subject', 'key_type': 'RANGE'},
            {'attribute_name': 'forum', 'key_type': 'HASH'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        schema.reverse()
        self.assertEqual(body['table_description']['key_schema'], schema)
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-21_3'])
    def test_create_table_b(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'B'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        desc = body['table_description']
        self.assertEqual(len(attr_def), len(desc['attribute_definitions']))
        for attribute in attr_def:
            self.assertIn(attribute, desc['attribute_definitions'])
        self.assertEqual(schema, desc['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-14'])
    def test_create_table_non_key_attr_s(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        additional_attr = [{'attribute_name': 'str', 'attribute_type': 'S'}]
        headers, body = self._create_test_table(
            self.smoke_attrs + additional_attr,
            tname,
            self.smoke_schema)
        desc = body['table_description']
        self.assertEqual(3, len(desc['attribute_definitions']))
        for attribute in (self.smoke_attrs + additional_attr):
            self.assertIn(attribute, desc['attribute_definitions'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-23'])
    def test_create_table_non_key_attr_ss(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        additional_attr = [{'attribute_name': 'set', 'attribute_type': 'SS'}]
        headers, body = self._create_test_table(
            self.smoke_attrs + additional_attr,
            tname,
            self.smoke_schema)
        desc = body['table_description']
        self.assertEqual(3, len(desc['attribute_definitions']))
        for attribute in (self.smoke_attrs + additional_attr):
            self.assertIn(attribute, desc['attribute_definitions'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-54'])
    def test_create_table_one_index(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        for request_index in request_lsi:
            request_index['index_size_bytes'] = 0
            request_index['item_count'] = 0
        self.assertEqual(request_lsi, indexes)
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-55'])
    def test_create_table_five_indexes(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        index_attrs = [{'attribute_name': 'attr_name' + str(i),
                        'attribute_type': 'S'} for i in range(0, 5)]
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
        headers, body = self._create_test_table(
            self.smoke_attrs + index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(5, len(indexes))
        for lsi in request_lsi:
            lsi['index_size_bytes'] = 0
            lsi['item_count'] = 0
            self.assertIn(lsi, indexes)
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-61'])
    def test_create_table_index_name_3_char(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'qqq'
        request_lsi[0]['index_name'] = request_index_name
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        self.assertEqual(request_index_name, indexes[0]['index_name'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-62'])
    def test_create_table_index_name_255_char(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'q' * 255
        request_lsi[0]['index_name'] = request_index_name
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        self.assertEqual(request_index_name, indexes[0]['index_name'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-65'])
    def test_create_table_index_name_upper_case(self):
        # TODO(aostapenko) clarify behavior on dynamodb
        tname = rand_name(self.table_prefix).replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'INDEX_NAME'
        request_lsi[0]['index_name'] = request_index_name
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        self.assertEqual(request_index_name, indexes[0]['index_name'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-66'])
    def test_create_table_index_name_same_two_tables(self):
        tname1 = rand_name(self.table_prefix).replace('-', '')
        tname2 = rand_name(self.table_prefix).replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'INDEX_NAME'
        request_lsi[0]['index_name'] = request_index_name
        headers1, body1 = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname1,
            self.smoke_schema,
            request_lsi)
        headers2, body2 = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname2,
            self.smoke_schema,
            request_lsi)
        self.assertEqual(dict, type(body1))
        self.assertEqual(dict, type(body2))
        indexes1 = body1['table_description']['local_secondary_indexes']
        indexes2 = body2['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes1))
        self.assertEqual(1, len(indexes2))
        self.assertEqual(request_index_name, indexes1[0]['index_name'])
        self.assertEqual(request_index_name, indexes2[0]['index_name'])

    @attr(type=['CreT-27'])
    def test_create_table_only_hash(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        headers, body = self._create_test_table(self.one_attr, tname,
                                                self.schema_hash_only)
        self.assertEqual(self.schema_hash_only,
                         body['table_description']['key_schema'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-81'])
    def test_create_table_index_1_non_key(self):
        tname = rand_name(self.table_prefix).replace('-', '')
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
                    'projection_type': 'INCLUDE',
                    'non_key_attributes': non_key_attributes}
            }
        ]
        headers, body = self._create_test_table(
            self.smoke_attrs + index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)

        indexes = body['table_description']['local_secondary_indexes']
        self.assertIn('non_key_attributes', indexes[0]['projection'])
        self.assertEqual(non_key_attributes,
                         indexes[0]['projection']['non_key_attributes'])
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-85'])
    def test_create_table_index_20_non_key_1_index(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        index_attrs = [{'attribute_name': 'attr_name1',
                        'attribute_type': 'S'},
                       ]
        non_key_attributes = ['non_key_attr' + str(i) for i in range(0, 20)]
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
        headers, body = self._create_test_table(
            self.smoke_attrs + index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        self.assertIn('non_key_attributes', indexes[0]['projection'])
        self.assertEqual(20,
                         len(indexes[0]['projection']['non_key_attributes']))
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-87'])
    def test_create_table_index_20_non_key_3_indexes(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        index_attrs = [{'attribute_name': 'attr_name' + str(i),
                        'attribute_type': 'S'} for i in range(1, 4)
                       ]
        non_key_attributes = (['non_key_attr' + str(i) for i in range(1, 8)],
                              ['non_key_attr' + str(i) for i in range(8, 15)],
                              ['non_key_attr' + str(i) for i in range(15, 21)])
        request_lsi = []
        for i, attr_name in enumerate(index_attrs):
            request_lsi.append(
                {
                    'index_name': '%s_index' % attr_name['attribute_name'],
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
        headers, body = self._create_test_table(
            self.smoke_attrs + index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        indexes = body['table_description']['local_secondary_indexes']
        for i in range(0, 3):
            self.assertIn('non_key_attributes', indexes[i]['projection'])
            index_name = indexes[i]['index_name']
            non_key_attrs = [
                lsi['projection']['non_key_attributes']
                for lsi in request_lsi if lsi['index_name'] == index_name
            ][0]
            self.assertEqual(len(non_key_attrs), len(indexes[i]['projection']
                                                     ['non_key_attributes']))
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))
