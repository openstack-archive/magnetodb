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


class MagnetoDBScanTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBScanTest, self).setUp()
        self.tname = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema,
                                 self.smoke_throughput)
        self.wait_for_table_active(self.tname)
        self.rck = self.addResourceCleanUp(self.client.delete_table,
                                           self.tname)

    def tearDown(self):
        super(MagnetoDBScanTest, self).tearDown()
        self.client.delete_table(self.tname)
        self.assertTrue(self.wait_for_table_deleted(self.tname))
        self.cancelResourceCleanUp(self.rck)

    def _verify_scan_response(self, response,
                              attributes_to_get=None, limit=None,
                              select=None, scan_filter=None,
                              exclusive_start_key=None,
                              return_consumed_capacity=None,
                              total_segments=None, segment=None):
        """
        The method implements common verifications of response format.

        More specific verifications should be implemented in the test-cases
        explicitly.
        """
        if return_consumed_capacity is None:
            self.assertNotIn('ConsumedCapacity', response)
        self.assertIn('ScannedCount', response)
        self.assertIn('Count', response)
        self.assertEqual(select == 'COUNT', 'Items' not in response)
        if select != 'COUNT':
            self.assertEqual(response['Count'], len(response['Items']))
        if limit is None:
            self.assertNotIn('LastEvaluatedKey', response)

    @attr(type='smoke')
    def test_scan(self):
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2')
        resp = self.client.scan(table_name=self.tname)
        self._verify_scan_response(resp)
        self.assertEqual(1, resp['Count'])
        self.assertEqual(item, resp['Items'][0])

    def test_scan_limit_scanfilter(self):
        """
        Verifies the scan operation processes all existing items,
        filters appropriate ones and stops scanning if the limit reached.
        """
        exp_scanned_count = 2 * 10
        exp_count = 10
        limit = 5
        items = self.populate_smoke_table(self.tname, 2, exp_count)

        scanfilter = {
            'message': {
                'ComparisonOperator': 'NOT_NULL'
            }
        }
        resp1 = self.client.scan(self.tname, scan_filter=scanfilter,
                                 limit=limit)

        self._verify_scan_response(response=resp1, limit=limit,
                                   scan_filter=scanfilter)
        self.assertEqual(limit, resp1['ScannedCount'])
        count1 = resp1['Count']
        last_key = resp1['LastEvaluatedKey']

        resp2 = self.client.scan(self.tname, scan_filter=scanfilter,
                                 exclusive_start_key=last_key)
        self.assertEqual(exp_scanned_count - limit, resp2['ScannedCount'])
        count2 = resp2['Count']
        self.assertEqual(exp_scanned_count, count1 + count2)
        for item in resp1['Items']:
            self.assertNotIn(item, resp2['Items'])

    def test_scan_attr_to_get_select(self):
        """
        Test the cases:
          - AttributesToGet contains non-key attr not used in filter;
          - AttributesToGet contains nonexistent attr;
          - Select=SPECIFIC_ATTRIBUTES and AttributesToGet specified;
          - Select=ALL_ATTRIBUTES;
          - Select=COUNT.
        """
        item1 = self.put_smoke_item(self.tname, 'forum', 'subject1',
                                    'filtered', 'John')
        self.put_smoke_item(self.tname, 'forum', 'subject2', 'skipped', 'Alex')

        attrs_to_get = ['last_posted_by', 'nonexistent_attr']
        scanfilter = {
            'message': {
                'AttributeValueList': [{'S': 'filter'}],
                'ComparisonOperator': 'CONTAINS'
            }
        }

        resp1 = self.client.scan(self.tname, scan_filter=scanfilter,
                                 attributes_to_get=attrs_to_get)
        self.assertEqual(1, resp1['Count'])

        exp_items = [{'last_posted_by': item1['last_posted_by']}]
        self.assertEqual(exp_items, resp1['Items'])

        # If Select=SPECIFIC_ATTRIBUTES and AttributesToGet are specified,
        # result should be equivalent to specifying AttributesToGet without
        # specifying any value for Select.
        resp2 = self.client.scan(self.tname, scan_filter=scanfilter,
                                 attributes_to_get=attrs_to_get,
                                 select='SPECIFIC_ATTRIBUTES')
        self.assertEqual(resp1, resp2)

        # verify results if Select set to get all attributes
        resp3 = self.client.scan(self.tname, scan_filter=scanfilter,
                                 select='ALL_ATTRIBUTES')
        self.assertEqual([item1], resp3['Items'])

        resp4 = self.client.scan(self.tname, select='COUNT')
        self._verify_scan_response(resp4, select='COUNT')
        self.assertEqual(2, resp4['Count'])
