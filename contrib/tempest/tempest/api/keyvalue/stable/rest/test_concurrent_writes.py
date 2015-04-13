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
from tempest_lib.common.utils.data_utils import rand_name
from tempest_lib import exceptions


class MagnetoDBConcurrentUpdateTestCase(rest_base.MagnetoDBConcurrentTestCase):

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_concurrent_writes_2_fields_expected(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'},
             {'attribute_name': 'Subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'},
             {'attribute_name': 'Subject', 'key_type': 'RANGE'}],
            wait_for_active=True)

        key = {
            "ForumName": {
                "S": "forum name",
            },
            "Subject": {
                "S": "subject",
            }
        }
        attribute_updates_list = [
            {
                "Tags": {
                    "action": "PUT",
                    "value": {
                        "S": "1.0.1"
                    }
                }
            },
            {
                "Author": {
                    "action": "PUT",
                    "value": {
                        "S": "John"
                    }
                }
            }
        ]
        expected_list = [
            {
                "Author": {
                    "exists": False
                }
            },
            {
                "Tags": {
                    "exists": False
                }
            }
        ]

        for attemts in xrange(0, 100):
            done_count = [0]
            done_event = threading.Event()
            exc_raised_once = [False]

            def callback(future):
                try:
                    future.result()
                except exceptions.BadRequest as e:
                    if 'ConditionalCheckFailedException' in e._error_string:
                        exc_raised_once[0] = not exc_raised_once[0]
                finally:
                    done_count[0] += 1
                    if done_count[0] >= len(attribute_updates_list):
                        done_event.set()

            for i in xrange(0, 2):
                future = self._async_request(
                    'update_item',
                    self.table_name,
                    key,
                    attribute_updates=attribute_updates_list[i],
                    expected=expected_list[i]
                )
                future.add_done_callback(callback)
            done_event.wait()
            self.client.delete_item(self.table_name, key)
            self.assertTrue(exc_raised_once)
