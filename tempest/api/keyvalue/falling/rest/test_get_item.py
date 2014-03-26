# Copyright 2014 Mirantis Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the 'License'); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import random
import string
from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase


class MagnetoDBPutGetItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBPutGetItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_get_item_long_table_name(self):
        self.table_name = self.random_name(255)
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(self.table_name, 'forum1', 'subject2')

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(self.table_name,
                                        key, attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})
