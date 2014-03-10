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
from tempest.openstack.common import log as logging
from tempest.test import attr


LOG = logging.getLogger(__name__)


class MagnetoDBItemsTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBItemsTest, cls).setUpClass()
        cls.tname = rand_name().replace('-', '')

    @attr(type='smoke')
    def test_put_get_update_delete_item(self):
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.tname,
                                 self.smoke_schema,
                                 self.smoke_throughput,
                                 self.smoke_lsi,
                                 self.smoke_gsi)
        self.wait_for_table_active(self.tname)
        self.addResourceCleanUp(self.client.delete_table, self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        resp = self.client.put_item(self.tname, item)
        self.assertEqual({}, resp)

        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        resp = self.client.get_item(self.tname, key)
        self.assertEqual(item, resp['Item'])

        attribute_updates = {
            'last_posted_by': {
                'Value': {'S': 'John Doe'},
                'Action': 'PUT'
            }
        }
        resp = self.client.update_item(self.tname, key, attribute_updates)
        self.assertEqual({}, resp)
        resp = self.client.get_item(self.tname, key, consistent_read=True)
        self.assertEqual(resp['Item']['last_posted_by'], {'S': 'John Doe'})

        resp = self.client.delete_item(self.tname, key)
        self.assertEqual({}, resp)
        self.assertEqual({}, self.client.get_item(self.tname, key,
                                                  consistent_read=True))

    def test_get_item_table_not_exists(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}
        self.assertBotoError(self.errors.client.ResourceNotFoundException,
                             self.client.get_item,
                             self.tname,
                             key)

    def test_get_item_table_creating(self):
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.tname,
                                 self.smoke_schema,
                                 self.smoke_throughput,
                                 self.smoke_lsi,
                                 self.smoke_gsi)
        self.addResourceCleanUp(self.client.delete_table, self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}
        self.assertBotoError(self.errors.client.ResourceNotFoundException,
                             self.client.get_item,
                             self.tname,
                             key)
        self.wait_for_table_active(self.tname)

    def test_get_item_bad_keys(self):
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.tname,
                                 self.smoke_schema,
                                 self.smoke_throughput,
                                 self.smoke_lsi,
                                 self.smoke_gsi)
        self.wait_for_table_active(self.tname)
        self.addResourceCleanUp(self.client.delete_table, self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        keys = [{self.hashkey: 'badhashkey',
                 self.rangekey: item[self.rangekey]},
                {self.hashkey: item[self.hashkey],
                 self.rangekey: 'badrangekey'},
                {self.hashkey: 'badhashkey',
                 self.rangekey: 'badrangekey'},
                ]
        for key in keys:
            self.assertBotoError(self.errors.client.SerializationException,
                                 self.client.get_item,
                                 self.tname,
                                 key)
        self.wait_for_table_active(self.tname)

    def test_get_item_empty_key(self):
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.tname,
                                 self.smoke_schema,
                                 self.smoke_throughput,
                                 self.smoke_lsi,
                                 self.smoke_gsi)
        self.wait_for_table_active(self.tname)
        self.addResourceCleanUp(self.client.delete_table, self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        keys = [{},
                {self.hashkey: item[self.hashkey]},
                {self.rangekey: item[self.rangekey]},
                ]
        for key in keys:
            self.assertBotoError(self.errors.client.ValidationException,
                                 self.client.get_item,
                                 self.tname,
                                 key)
        self.wait_for_table_active(self.tname)
