# Copyright 2014 Mirantis Inc.
# Copyright 2014 Symantec Corporation
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

from tempest.test import attr
from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest import exceptions


class MagnetoDBPutItemTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBPutItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type='PI-81')
    def test_put_item_with_returned_all_old(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "authors": {"SS": ["Alice", "Bob"]},
            "others": {"SS": ["qqqq", "wwww"]}
        }
        new_item = {
            "message": {"S": "message_text"},
            "authors": {"SS": ["Kris", "Rob"]},
            "others": {"SS": ["zzzz", "xxxx"]}
        }
        put_resp = self.client.put_item(self.table_name,
                                        item,
                                        None,
                                        0,
                                        "ALL_OLD")
        self.assertEqual(put_resp[1], {})
        put_resp = self.client.put_item(self.table_name,
                                        new_item,
                                        None,
                                        0,
                                        "ALL_OLD")
        self.assertEqual(put_resp[1]["attributes"]["message"]["S"],
                         "message_text")
        self.assertEqual(set(put_resp[1]["attributes"]["authors"]["SS"]),
                         {"Alice", "Bob"})
        self.assertEqual(set(put_resp[1]["attributes"]["others"]["SS"]),
                         {"qqqq", "wwww"})

    @attr(type=['PI-102', 'negative'])
    def test_put_item_in_table_with_wrong_name(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item("", item)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "2 validation errors detected: Value '' at"
                         " 'tableName' failed to satisfy constraint:"
                         " Member must satisfy regular expression pattern:"
                         " [a-zA-Z0-9_.-]+; Value '' at 'tableName' failed"
                         " to satisfy constraint: Member must have length"
                         " greater than or equal to 3")
        self.assertIn("ValidationException", exception.body["__type"])

    @attr(type=['PI-103', 'negative'])
    def test_put_item_in_table_with_short_name(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item("qw", item)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "1 validation error detected: Value 'qw' at"
                         " 'tableName' failed to satisfy constraint:"
                         " Member must have length greater than or equal to 3")
        self.assertIn("ValidationException", exception.body["__type"])

    @attr(type=['PI-104', 'negative'])
    def test_put_item_in_table_with_long_name(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        name_longer_than_255_characters = self.random_name(256)

        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(name_longer_than_255_characters, item)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "1 validation error detected: Value" +
                         name_longer_than_255_characters +
                         "at 'tableName' failed to satisfy constraint:"
                         " Member must have length less than or equal to 255")
        self.assertIn("ValidationException", exception.body["__type"])
