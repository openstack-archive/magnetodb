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

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBCreateTableTestCase(MagnetoDBTestCase):

    def _verify_table_response(self, response, attributes, table_name,
                               key_schema, local_secondary_indexes=None):

        self.assertTrue('table_description' in response)
        content = response['table_description']
        self.assertEqual(table_name, content['table_name'])
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

    @attr(type=['CreT-27'])
    def test_create_table_only_hash(self):
        tname = rand_name().replace('-', '')
        headers, body = self._create_test_table(self.one_attr, tname,
                                                self.schema_hash_only)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body,
                                    self.one_attr, tname,
                                    self.schema_hash_only)

    @attr(type=['CreT-48'])
    def test_create_table_symbols(self):
        tname = 'Aa5-._'
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(body['table_description']['table_name'], tname)

    @attr(type=['CreT-45'])
    def test_create_table_max_table_name(self):
        tname = rand_name().replace('-', '')
        tname = tname + 'q' * (255 - len(tname))
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, self.smoke_attrs,
                                    tname, self.smoke_schema)

    @attr(type=['CreT-81'])
    def test_create_table_index_1_non_key(self):
        tname = rand_name().replace('-', '')
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
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertIn('non_key_attributes', indexes[0]['projection'])
        self.assertEqual(non_key_attributes,
                         indexes[0]['projection']['non_key_attributes'])

    @attr(type=['CreT-85'])
    def test_create_table_index_20_non_key_1_index(self):
        tname = rand_name().replace('-', '')
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
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertIn('non_key_attributes', indexes[0]['projection'])
        self.assertEqual(20,
                         len(indexes[0]['projection']['non_key_attributes']))

    @attr(type=['CreT-87'])
    def test_create_table_index_20_non_key_3_indexes(self):
        tname = rand_name().replace('-', '')
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
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        for i in range(0, 3):
            self.assertIn('non_key_attributes', indexes[i]['projection'])
            self.assertEqual(len(non_key_attributes[i]),
                             len(indexes[i]['projection']
                                        ['non_key_attributes']))
