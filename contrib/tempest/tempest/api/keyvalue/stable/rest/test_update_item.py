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
from tempest_lib.common.utils.data_utils import rand_name


class MagnetoDBUpdateItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBUpdateItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_update_item_non_existent_item_add_action(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        update_resp = self.client.update_item(
            self.table_name, key, attribute_updates=attribute_updates,
            expected=None, time_to_live=None, return_values=None)

        self.assertEqual(update_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"ForumName": {"S": 'forum name'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['ForumName'],
                         {'S': 'forum name'})
        self.assertEqual(set(get_resp[1]['item']['Tags']['SS']),
                         {'tag set value'})

    def test_update_item_non_existent_item_mixed_actions(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'},
             {'attribute_name': 'Subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'}],
            wait_for_active=True)
        key2 = {
            "ForumName": {
                "S": "forum name 2"
            }
        }
        attribute_updates2 = {
            "LastPostedBy": {
                "action": "PUT",
                "value": {
                    "S": "user1@test.com"
                }
            },
            "Tags": {
                "action": "ADD",
                "value": {
                    "SS": ["tag set value 1",
                           "tag set value 2"]
                }
            },
            "AdditionalAttribute": {
                "action": "DELETE",
                "value": {
                    "S": "additional attribute value"
                }
            }
        }
        update_resp2 = self.client.update_item(
            self.table_name, key2, attribute_updates=attribute_updates2,
            expected=None, time_to_live=None, return_values=None)

        self.assertEqual(update_resp2[1], {})
        get_resp2 = self.client.get_item(self.table_name,
                                         {"ForumName": {"S": 'forum name 2'}},
                                         consistent_read=True)
        self.assertEqual(get_resp2[1]['item']['ForumName'],
                         {'S': 'forum name 2'})
        self.assertEqual(get_resp2[1]['item']['LastPostedBy'],
                         {'S': 'user1@test.com'})
        self.assertEqual(set(get_resp2[1]['item']['Tags']['SS']),
                         {'tag set value 1', 'tag set value 2'})

    def test_update_item_non_existent_item_delete_only_actions(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'},
             {'attribute_name': 'Subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'}],
            wait_for_active=True)
        key3 = {
            "ForumName": {
                "S": "forum name 3"
            }
        }
        attribute_updates3 = {
            "LastPostedBy": {
                "action": "DELETE",
                "value": {
                    "S": "user1@test.com"
                }
            },
            "Tags": {
                "action": "DELETE",
                "value": {
                    "SS": ["tag set value 3"]
                }
            },
            "AdditionalAttribute": {
                "action": "DELETE"
            }
        }
        update_resp3 = self.client.update_item(
            self.table_name, key3, attribute_updates=attribute_updates3,
            expected=None, time_to_live=None, return_values=None)

        self.assertEqual(update_resp3[1], {})
        get_resp3 = self.client.get_item(self.table_name,
                                         {"ForumName": {"S": 'forum name 3'}},
                                         consistent_read=True)
        # DELETE only actions should be ignored. Response should be empty.
        self.assertEqual(get_resp3[1], {})

    def test_update_item_add_existing_set(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'},
             {'attribute_name': 'Tags', 'attribute_type': 'SS'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'}],
            wait_for_active=True)
        key = {
            "ForumName": {
                "S": "forum name"
            }
        }
        item = {
            "ForumName": {"S": "forum name"},
            "Tags": {"SS": ["tag set value 1",
                            "tag set value 2",
                            "tag set value 3"]},
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})

        get_resp = self.client.get_item(self.table_name,
                                        {"ForumName": {"S": 'forum name'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['ForumName'],
                         {'S': 'forum name'})
        self.assertEqual(set(get_resp[1]['item']['Tags']['SS']),
                         {"tag set value 1", "tag set value 2",
                          "tag set value 3"})

        # add an old value and a new value to set, old value should be ignored
        attribute_updates = {
            "Tags": {
                "action": "ADD",
                "value": {
                    "SS": ["tag set new value 1",
                           "tag set value 2"]
                }
            }
        }
        update_resp = self.client.update_item(
            self.table_name, key, attribute_updates=attribute_updates,
            expected=None, time_to_live=None, return_values=None)

        self.assertEqual(update_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"ForumName": {"S": 'forum name'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['ForumName'],
                         {'S': 'forum name'})
        self.assertEqual(set(get_resp[1]['item']['Tags']['SS']),
                         {"tag set value 1", "tag set value 2",
                          "tag set value 3", "tag set new value 1"})

    def test_update_item_existing_item_with_only_key_attrs(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'}],
            wait_for_active=True)
        key = {
            "ForumName": {
                "S": "forum name"
            }
        }
        attribute_updates = {
            "Subject": {
                "action": "PUT",
                "value": {
                    "S": "subject"
                }
            }
        }
        self.client.put_item(self.table_name, key)
        headers, body = self.client.update_item(
            self.table_name, key, attribute_updates=attribute_updates)
        self.assertEqual({}, body)

    def test_update_item_non_existent_all_old(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'ForumName', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'ForumName', 'key_type': 'HASH'}],
            wait_for_active=True)
        key = {
            "ForumName": {
                "S": "forum name"
            }
        }
        attribute_updates = {
            "Subject": {
                "action": "PUT",
                "value": {
                    "S": "subject"
                }
            }
        }

        headers, body = self.client.update_item(
            self.table_name, key, attribute_updates=attribute_updates,
            return_values="ALL_OLD")

        self.assertEqual({'attributes': {}}, body)
