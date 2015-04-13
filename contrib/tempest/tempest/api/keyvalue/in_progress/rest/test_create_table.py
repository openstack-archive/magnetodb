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

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
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

    @attr(type=['CreT-45'])
    def test_create_table_max_table_name(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        tname = tname + 'q' * (255 - len(tname))
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(body['table_description']['table_name'], tname)
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))

    @attr(type=['CreT-48'])
    def test_create_table_symbols(self):
        tname = rand_name(self.table_prefix).replace('-', '') + 'Aa5-._'
        headers, body = self._create_test_table(self.smoke_attrs, tname,
                                                self.smoke_schema)
        self.assertEqual(body['table_description']['table_name'], tname)
        # NOTE(aostapenko) we can't guarantee that FAIL occures because of test
        # theme, no info about error cause from back-end is available
        self.assertTrue(self.wait_for_table_active(tname))
