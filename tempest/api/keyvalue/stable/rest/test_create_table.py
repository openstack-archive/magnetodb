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

import copy

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBCreateTableTestCase(MagnetoDBTestCase):

    def _verify_table_response(self, method, response,
                               attributes, table_name,
                               key_schema,
                               local_secondary_indexes=None):

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

    @attr(type=['CreT-1'])
    def test_create_table(self):
        tname = rand_name().replace('-', '')
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, self.smoke_attrs,
                                    tname, self.smoke_schema)

    @attr(type=['CreT-44'])
    def test_create_table_min_table_name(self):
        tname = 'qqq'
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(dict, type(body))
        self.assertEqual(tname, body['table_description']['table_name'])

    @attr(type=['CreT-2', 'Cret-93', 'Cret-94', 'Cret-95', 'Cret-97'])
    def test_create_table_all_params(self):
        tname = rand_name().replace('-', '')
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            self.smoke_lsi)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table',
                                    body,
                                    self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.smoke_schema,
                                    self.smoke_lsi)

    @attr(type=['CreT-17_1'])
    def test_create_table_min_attr_length(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'f', 'attribute_type': 'S'},
            {'attribute_name': 's', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'f', 'key_type': 'HASH'},
            {'attribute_name': 's', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

    @attr(type=['CreT-17_2'])
    def test_create_table_max_attr_length(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'f' * 255, 'attribute_type': 'S'},
            {'attribute_name': 's' * 255, 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'f' * 255, 'key_type': 'HASH'},
            {'attribute_name': 's' * 255, 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

    @attr(type=['CreT-21_1'])
    def test_create_table_s(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'S'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

    @attr(type=['CreT-21_2'])
    def test_create_table_n(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

    @attr(type=['CreT-28'])
    def test_create_table_hash_range(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self.assertEqual(schema, body['table_description']['key_schema'])
        self.assertEqual(attr_def,
                         body['table_description']['attribute_definitions'])

    @attr(type=['CreT-29'])
    def test_create_table_wrong_order(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'subject', 'key_type': 'RANGE'},
            {'attribute_name': 'forum', 'key_type': 'HASH'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        schema.reverse()
        self.assertEqual(body['table_description']['key_schema'], schema)

    @attr(type=['CreT-21_3'])
    def test_create_table_b(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'B'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        headers, body = self._create_test_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

    @attr(type=['CreT-14'])
    def test_create_table_non_key_attr_s(self):
        tname = rand_name().replace('-', '')
        additional_attr = [{'attribute_name': 'str', 'attribute_type': 'S'}]
        headers, body = self._create_test_table(
            self.smoke_attrs + additional_attr,
            tname,
            self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body,
                                    self.smoke_attrs + additional_attr,
                                    tname, self.smoke_schema)

    @attr(type=['CreT-23'])
    def test_create_table_non_key_attr_ss(self):
        tname = rand_name().replace('-', '')
        additional_attr = [{'attribute_name': 'set', 'attribute_type': 'SS'}]
        headers, body = self._create_test_table(
            self.smoke_attrs + additional_attr,
            tname,
            self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body,
                                    self.smoke_attrs + additional_attr,
                                    tname, self.smoke_schema)

    @attr(type=['CreT-54'])
    def test_create_table_one_index(self):
        tname = rand_name().replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        for request_index in request_lsi:
            request_index['index_size_bytes'] = 0
            request_index['item_count'] = 0
        self.assertEqual(request_lsi, indexes)

    @attr(type=['CreT-55'])
    def test_create_table_five_indexes(self):
        tname = rand_name().replace('-', '')
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
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(5, len(indexes))
        for lsi in request_lsi:
            lsi['index_size_bytes'] = 0
            lsi['item_count'] = 0
            self.assertIn(lsi, indexes)

    @attr(type=['CreT-61'])
    def test_create_table_index_name_3_char(self):
        tname = rand_name().replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'qqq'
        request_lsi[0]['index_name'] = request_index_name
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        self.assertEqual(request_index_name, indexes[0]['index_name'])

    @attr(type=['CreT-62'])
    def test_create_table_index_name_255_char(self):
        tname = rand_name().replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'q' * 255
        request_lsi[0]['index_name'] = request_index_name
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        self.assertEqual(request_index_name, indexes[0]['index_name'])

    @attr(type=['CreT-65'])
    def test_create_table_index_name_upper_case(self):
        # TODO(aostapenko) clarify behavior on dynamodb
        tname = rand_name().replace('-', '')
        request_lsi = copy.deepcopy(self.smoke_lsi)
        request_index_name = 'INDEX_NAME'
        request_lsi[0]['index_name'] = request_index_name
        headers, body = self._create_test_table(
            self.smoke_attrs + self.index_attrs,
            tname,
            self.smoke_schema,
            request_lsi)
        self.assertEqual(dict, type(body))
        indexes = body['table_description']['local_secondary_indexes']
        self.assertEqual(1, len(indexes))
        self.assertEqual(request_index_name, indexes[0]['index_name'])

    @attr(type=['CreT-66'])
    def test_create_table_index_name_same_two_tables(self):
        tname1 = rand_name().replace('-', '')
        tname2 = rand_name().replace('-', '')
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
