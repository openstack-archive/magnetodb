# Copyright 2014 Symantec Corporation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License'); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import random
import string

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name


class MagnetoDBUpdateItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBUpdateItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_update_item_non_existent_item(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'},
             {'attribute_name': 'Subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'}],
            wait_for_active=True)
        key = {
            "ForumName": {
                "S": "forum name"
            }
        }
        attribute_updates = {
            "Tags": {
                "action": "ADD",
                "value": {
                    "SS": ["tag set value"]
                }
            }
        }
        update_resp = self.client.update_item(self, self.table_name,
                                              key, attribute_updates=attribute_updates,
                                              expected=None, time_to_live=None,
                                              return_values=None)

        self.assertEqual(update_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"ForumName": {"S": 'forum name'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['ForumName'],
                         {'S': 'forum name'})
        self.assertEqual(get_resp[1]['item']['Tags'],
                         {'SS': ['tag set value']})
