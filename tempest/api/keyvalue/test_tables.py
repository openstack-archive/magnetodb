# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 OpenStack Foundation
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

from tempest.api.keyvalue.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBTablesTest(MagnetoDBTestCase):

    def _verify_create_table_response(self, response,
                                      attributes, table_name,
                                      key_schema, provisioned_throughput,
                                      local_secondary_indexes=None,
                                      global_secondary_indexes=None):

        self.assertTrue('TableDescription' in response)
        content = response['TableDescription']
        self.assertEqual(table_name, content['TableName'])
        self.assertEqual(attributes, content['AttributeDefinitions'])
        self.assertEqual(0, content['ItemCount'])
        self.assertEqual(key_schema, content['KeySchema'])
        self.assertEqual(0, content['TableSizeBytes'])

        expected = provisioned_throughput.copy()
        expected['NumberOfDecreasesToday'] = 0
        self.assertEqual(expected, content['ProvisionedThroughput'])

        if local_secondary_indexes is None:
            self.assertNotIn('LocalSecondaryIndexes', content)
        else:
            expected = local_secondary_indexes.copy()
            expected['IndexSizeBytes'] = 0
            expected['IndexStatus'] = 'CREATING'
            expected['ItemCount'] = 0
            self.assertEqual(expected, content['LocalSecondaryIndexes'])

        if global_secondary_indexes is None:
            self.assertNotIn('GlobalSecondaryIndexes', content)
        else:
            expected = global_secondary_indexes.copy()
            # TODO(yyekovenko): Check all attributes of GSI
            self.assertEqual(expected, content['GlobalSecondaryIndexes'])

        # CreationDateTime
        # TODO(yyekovenko): 0 returned for now. Find a way to get exp. time
        self.assertIn('CreationDateTime', content)

        # TODO(yyekovenko): Later should be changed to just "CREATING" (async)
        self.assertIn(content['TableStatus'], ['CREATING', 'ACTIVE'])

    @attr(type='smoke')
    def test_create_list_delete_table(self):
        tname = rand_name().replace('-', '')
        table = self.client.create_table(self.smoke_attrs + self.index_attrs,
                                         tname,
                                         self.smoke_schema,
                                         self.smoke_throughput,
                                         self.smoke_lsi,
                                         self.smoke_gsi)
        rck = self.addResourceCleanUp(self.client.delete_table, tname)
        self.assertEqual(dict, type(table))
        # TODO(yyekovenko) Later should be changed to just "CREATING" (async)
        self.assertIn(table['TableDescription']['TableStatus'],
                      ['CREATING', 'ACTIVE'])
        self.assertEqual(tname, table['TableDescription']['TableName'])
        self.assertTrue(self.wait_for_table_active(tname))

        tables = self.client.list_tables()
        self.assertIn(tname, tables['TableNames'])

        descr = self.client.describe_table(tname)
        self.assertIn('Table', descr)
        self.assertIn('ItemCount', descr['Table'])

        res = self.client.delete_table(tname)
        # TODO(yyekovenko) Later should be changed to just "DELETING" (async)
        self.assertIn(res['TableDescription']['TableStatus'],
                      ['DELETING', 'ACTIVE'])
        self.assertTrue(self.wait_for_table_deleted(tname))
        self.cancelResourceCleanUp(rck)

    def test_create_table_with_required_params_only(self):
        """
        Create a table only with required attrs and verify response content
        """
        tname = rand_name().replace('-', '')
        table = self.client.create_table(
            attribute_definitions=self.smoke_attrs,
            table_name=tname,
            key_schema=self.smoke_schema,
            provisioned_throughput=self.smoke_throughput)
        self.addResourceCleanUp(self.client.delete_table, tname)

        self._verify_create_table_response(
            table,
            self.smoke_attrs, tname, self.smoke_schema,
            self.smoke_throughput)

        self.assertTrue(self.wait_for_table_active(tname))

    def test_duplicate_table(self):
        tname = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs, tname,
                                 self.smoke_schema,
                                 self.smoke_throughput)
        self.assertTrue(self.wait_for_table_active(tname))
        self.addResourceCleanUp(self.client.delete_table, tname)

        self.assertBotoError(self.errors.client.ResourceInUseException,
                             self.client.create_table,
                             self.smoke_attrs, tname,
                             self.smoke_schema, self.smoke_throughput)

        new_tables = [n for n in self.client.list_tables()['TableNames']
                      if n == tname]
        self.assertEqual(1, len(new_tables))
