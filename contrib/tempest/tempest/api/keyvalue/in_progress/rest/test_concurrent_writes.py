# Copyright 2015 Symantec Corporation
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
import threading

from tempest.api.keyvalue.rest_base import base as rest_base
from tempest.common.utils.data_utils import rand_name


class MagnetoDBConcurrentUpdateTestCase(rest_base.MagnetoDBConcurrentTestCase):

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_concurrent_writes_200_fields(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'hash_attr', 'attribute_type': 'S'},
             {'attribute_name': 'range_attr', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'hash_attr', 'key_type': 'HASH'},
             {'attribute_name': 'range_attr', 'key_type': 'RANGE'}],
            wait_for_active=True)

        key = {
            "hash_attr": {
                "S": "hash_value",
            },
            "range_attr": {
                "S": "range_value",
            }
        }
        updates_count = 200
        attribute_updates_list = [
            {
                "extra_attr" + str(i): {
                    "action": "PUT",
                    "value": {
                        "S": "extra_value" + str(i)
                    }
                }
            } for i in xrange(0, updates_count)
        ]
        done_count = [0]
        done_event = threading.Event()

        def callback(future):
            try:
                future.result()
            finally:
                done_count[0] += 1
                if done_count[0] >= updates_count:
                    done_event.set()

        for attribute_updates in attribute_updates_list:
            future = self._async_request(
                'update_item',
                self.table_name,
                key,
                attribute_updates=attribute_updates
            )
            future.add_done_callback(callback)
        done_event.wait()
        headers, body = self.client.get_item(self.table_name, key)
        expected_item = key
        for attribute_updates in attribute_updates_list:
            k = attribute_updates.keys()[0]
            v = attribute_updates[k]["value"]
            expected_item.update({k: v})
        self.assertEqual(expected_item, body["item"])
