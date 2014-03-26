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
from tempest.openstack.common import log as logging
from tempest import test

LOG = logging.getLogger(__name__)


class MagnetoDBUpdateTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBUpdateTest, cls).setUpClass()

    def _create_table_for_test(self, wait):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema,
                                 self.smoke_throughput,
                                 self.smoke_lsi,
                                 self.smoke_gsi)
        if wait:
            self.wait_for_table_active(self.table_name)
        self.addResourceCleanUp(self.client.delete_table, self.table_name)

    @test.attr(type='negative')
    def test_update_item_in_nonexistent_table(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}
        attribute_updates = {
            'last_posted': {
                'Value': {'S': 'John Doe'},
                'Action': 'PUT'
            }
        }
        self.assertBotoError(self.errors.client.ResourceNotFoundException,
                             self.client.update_item,
                             'nonexistent_table',
                             key,
                             attribute_updates)

    @test.attr(type='negative')
    def test_update_table_creating(self):
        self._create_table_for_test(False)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}
        attribute_updates = {
            'last_posted': {
                'Value': {'S': 'John Doe'},
                'Action': 'PUT'
            }
        }
        self.assertBotoError(self.errors.client.ResourceNotFoundException,
                             self.client.update_item,
                             self.table_name,
                             key,
                             attribute_updates)
        self.wait_for_table_active(self.table_name)

    @test.attr(type='negative')
    def test_update_item_with_wrong_expected(self):
        self._create_table_for_test(True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}
        attribute_updates = {
            'last_posted': {
                'Value': {'S': 'John Doe'},
                'Action': 'PUT'
            }
        }
        expected = {
            'last_posted': {
                'Exists': 'false'
            }
        }
        resp = self.client.update_item(self.table_name, key, attribute_updates)
        self.assertEqual({}, resp)
        self.assertBotoError(
            self.errors.client.ConditionalCheckFailedException,
            self.client.update_item,
            self.table_name,
            key,
            attribute_updates,
            expected)

    @test.attr(type='negative')
    def test_update_item_bad_keys(self):
        self._create_table_for_test(True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        keys = [{self.hashkey: 'badhashkey',
                 self.rangekey: item[self.rangekey]},
                {self.hashkey: item[self.hashkey],
                 self.rangekey: 'badrangekey'},
                {self.hashkey: 'badhashkey',
                 self.rangekey: 'badrangekey'}, ]
        attribute_updates = {
            'last_posted': {
                'Value': {'S': 'John Doe'},
                'Action': 'PUT'
            }
        }

        for key in keys:
            self.assertBotoError(self.errors.client.SerializationException,
                                 self.client.update_item,
                                 self.table_name,
                                 key,
                                 attribute_updates)

    @test.attr(type='negative')
    def test_update_item_empty_key(self):
        self._create_table_for_test(True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        attribute_updates = {
            'last_posted': {
                'Value': {'S': 'John Doe'},
                'Action': 'PUT'
            }
        }
        keys = [{},
                {self.hashkey: item[self.hashkey]},
                {self.rangekey: item[self.rangekey]}, ]
        for key in keys:
            self.assertBotoError(self.errors.client.ValidationException,
                                 self.client.update_item,
                                 self.table_name,
                                 key,
                                 attribute_updates)
