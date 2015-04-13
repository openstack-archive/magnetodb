# Copyright 2014 Mirantis Inc.
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
from tempest_lib.common.utils.data_utils import rand_name


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
        tname = rand_name(self.table_prefix).replace('-', '')
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

    def test_create_table_index_20_non_key_3_indexes(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        index_attrs = [{'AttributeName': 'attr_name' + str(i),
                        'AttributeType': 'S'} for i in range(1, 4)
                       ]
        non_key_attributes = (['non_key_attr' + str(i) for i in range(1, 8)],
                              ['non_key_attr' + str(i) for i in range(8, 15)],
                              ['non_key_attr' + str(i) for i in range(15, 21)])
        request_lsi = []
        for i, attr_name in enumerate(index_attrs):
            request_lsi.append(
                {
                    'IndexName': '%s_index' % attr_name['AttributeName'],
                    'KeySchema': [
                        {'AttributeName': self.hashkey, 'KeyType': 'HASH'},
                        {'AttributeName': attr_name['AttributeName'],
                         'KeyType': 'RANGE'}
                    ],
                    'Projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': non_key_attributes[i]}
                }
            )
        body = self.client.create_table(
            self.smoke_attrs + index_attrs,
            tname,
            self.smoke_schema,
            self.smoke_throughput,
            request_lsi)
        self.assertTrue(self.wait_for_table_active(tname))
        self.addResourceCleanUp(self.client.delete_table, tname)
        indexes = body['TableDescription']['LocalSecondaryIndexes']

        for i in range(0, 3):
            self.assertIn('NonKeyAttributes', indexes[i]['Projection'])
            index_name = indexes[i]['IndexName']
            non_key_attrs = [
                lsi['Projection']['NonKeyAttributes']
                for lsi in request_lsi if lsi['IndexName'] == index_name
            ][0]
            self.assertEqual(len(non_key_attrs), len(indexes[i]['Projection']
                                                     ['NonKeyAttributes']))
