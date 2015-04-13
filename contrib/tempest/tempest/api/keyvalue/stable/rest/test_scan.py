# Copyright 2014 Mirantis Inc.
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


class MagnetoDBScanTestCase(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBScanTestCase, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    def test_scan_by_hash_eq(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
                                   'message text', 'John', '10')

        scan_filter = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
        }
        headers, body = self.client.scan(table_name=self.tname,
                                         scan_filter=scan_filter)
        self.assertIn('items', body)
        self.assertEqual(1, len(body['items']))
        self.assertIn(item, body['items'])

    def test_scan_by_range_eq(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
                                   'message text', 'John', '10')

        scan_filter = {
            'subject': {
                'attribute_value_list': [{'S': 'subject2'}],
                'comparison_operator': 'EQ'
            },
        }
        headers, body = self.client.scan(table_name=self.tname,
                                         scan_filter=scan_filter)
        self.assertIn('items', body)
        self.assertEqual(1, len(body['items']))
        self.assertIn(item, body['items'])
