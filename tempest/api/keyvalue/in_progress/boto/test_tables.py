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

from tempest.api.keyvalue.boto_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name


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
        for key, value in expected.items():
            self.assertIn(key, content['ProvisionedThroughput'])
            self.assertIsInstance(value, int)

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

    def test_duplicate_table(self):
        tname = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs, tname,
                                 self.smoke_schema,
                                 self.smoke_throughput)
        self.assertTrue(self.wait_for_table_active(tname))
        self.addResourceCleanUp(self.client.delete_table, tname)

        self.assertBotoError(
            self.errors.client.ResourceInUseException_DuplicateTable,
            self.client.create_table,
            self.smoke_attrs, tname,
            self.smoke_schema, self.smoke_throughput)

        new_tables = [n for n in self.client.list_tables()['TableNames']
                      if n == tname]
        self.assertEqual(1, len(new_tables))
