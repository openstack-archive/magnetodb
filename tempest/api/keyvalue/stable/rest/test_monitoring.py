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

from tempest import exceptions
from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBGetItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBGetItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type='GI-1')
    def test_get_item_correct_mandatory_attributes(self):
        table_name = rand_name().replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        mon_resp = self.client.monitoring(table_name)
        print(mon_resp)