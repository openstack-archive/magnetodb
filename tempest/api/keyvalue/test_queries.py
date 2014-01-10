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


class MagnetoDBQueriesTest(MagnetoDBTestCase):

    tname = None

    def setUp(self):
        super(MagnetoDBQueriesTest, self).setUp()
        if self.tname is None:
            self.tname = rand_name().replace('-', '')
            self.client.create_table(self.smoke_attrs,
                                     self.tname,
                                     self.smoke_schema,
                                     self.smoke_throughput,
                                     self.smoke_lsi,
                                     self.smoke_gsi)
            self.assertTrue(self.wait_for_table_active(self.tname))
            self.addResourceCleanUp(self.client.delete_table, self.tname)

    def test_query(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)

        key_conditions = {
            'forum': {
                'AttributeValueList': [{'S': 'forum1'}],
                'ComparisonOperator': 'EQ'
            },
            'subject': {
                'AttributeValueList': [{'S': 'subject'}],
                'ComparisonOperator': 'BEGINS_WITH'
            }
        }
        resp = self.client.query(table_name=self.tname,
                                 key_conditions=key_conditions)
        self.assertTrue(resp['Count'] > 0)

    def test_query_limit(self):
        items = self.populate_smoke_table(self.tname, 1, 10)

        key_conditions = {
            'forum': {
                'AttributeValueList': [items[0]['forum']],
                'ComparisonOperator': 'EQ'
            },
            'subject': {
                'AttributeValueList': [{'S': 'subject'}],
                'ComparisonOperator': 'BEGINS_WITH'
            }
        }

        resp1 = self.client.query(table_name=self.tname,
                                  key_conditions=key_conditions,
                                  limit=2)
        self.assertEqual(resp1['Count'], 2)
        last = resp1['LastEvaluatedTableName']
        # query remaining records
        resp2 = self.client.query(table_name=self.tname,
                                  key_conditions=key_conditions,
                                  exclusive_start_key=last)
        self.assertEqual(resp2['Count'], 8)
        self.assertNotIn(resp1['Items'][0], resp2['Items'])
        self.assertNotIn(resp1['Items'][1], resp2['Items'])
