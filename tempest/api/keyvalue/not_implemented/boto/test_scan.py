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
