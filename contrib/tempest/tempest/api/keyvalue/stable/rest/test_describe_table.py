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


class MagnetoDBDescribeTableTestCase(MagnetoDBTestCase):

    def _verify_table_response(self, response, attributes, table_name,
                               key_schema, local_secondary_indexes=None):

        self.assertTrue('table' in response)
        content = response['table']
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

    def test_describe_table(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs, tname, self.smoke_schema)
        headers, body = self.client.describe_table(tname)
        self.assertEqual(dict, type(body))
        self._verify_table_response(body, self.smoke_attrs, tname,
                                    self.smoke_schema)

    def test_describe_table_only_hash_key(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs, tname,
                                self.schema_hash_only,
                                wait_for_active=True)
        headers, body = self.client.describe_table(tname)
        self.assertEqual(dict, type(body))
        self._verify_table_response(body,
                                    self.smoke_attrs,
                                    tname,
                                    self.schema_hash_only)

    def test_describe_table_with_indexes(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        headers, body = self.client.describe_table(tname)
        self.assertEqual(dict, type(body))
        self._verify_table_response(body,
                                    self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.smoke_schema,
                                    self.smoke_lsi)
