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
        cls.client.create_table(cls.smoke_attrs + cls.index_attrs,
                                cls.tname,
                                cls.smoke_schema,
                                cls.smoke_throughput,
                                cls.smoke_lsi,
                                cls.smoke_gsi)
        cls.wait_for_table_active(cls.tname)
        cls.addResourceCleanUp(cls.client.delete_table, cls.tname)

    @attr(type='smoke')
    def test_put_get_update_delete_item(self):
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
