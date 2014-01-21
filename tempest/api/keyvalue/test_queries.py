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


class MagnetoDBQueriesTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBQueriesTest, cls).setUpClass()
        cls.tname = rand_name().replace('-', '')
        cls.client.create_table(cls.smoke_attrs,
                                cls.tname,
                                cls.smoke_schema,
                                cls.smoke_throughput)
        cls.addResourceCleanUp(cls.client.delete_table, cls.tname)

    @attr(type='smoke')
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
        self.assertEqual(2, resp1['Count'])
        last = resp1['LastEvaluatedKey']
        # query remaining records
        resp2 = self.client.query(table_name=self.tname,
                                  key_conditions=key_conditions,
                                  exclusive_start_key=last)
        self.assertEqual(8, resp2['Count'])
        self.assertNotIn(resp1['Items'][0], resp2['Items'])
        self.assertNotIn(resp1['Items'][1], resp2['Items'])
