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


class MagnetoDBTablesTest(MagnetoDBTestCase):

    def _verify_table_response(self, method, response,
                               attributes, table_name,
                               key_schema,
                               local_secondary_indexes=None):

        if method == 'create_table':
            self.assertTrue('table_description' in response)
            content = response['table_description']
        elif method == 'describe_table':
            self.assertTrue('table' in response)
            content = response['table']
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

    def test_create_table(self):
        tname = rand_name().replace('-', '')
        headers, body = self.client.create_table(self.smoke_attrs, tname,
                                                 self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, self.smoke_attrs,
                                    tname, self.smoke_schema)

    def test_create_table_min_table_name(self):
        tname = 'qqq'
        headers, body = self.client.create_table(self.smoke_attrs, tname,
                                                 self.smoke_schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, self.smoke_attrs,
                                    tname, self.smoke_schema)

    def test_create_table_with_indexes(self):
        tname = rand_name().replace('-', '')
        headers, body = self.client.create_table(
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
        headers, body = self.client.create_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

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
        headers, body = self.client.create_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

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
        headers, body = self.client.create_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)

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
        headers, body = self.client.create_table(attr_def, tname, schema)
        self.assertEqual(dict, type(body))
        self._verify_table_response('create_table', body, attr_def, tname,
                                    schema)
