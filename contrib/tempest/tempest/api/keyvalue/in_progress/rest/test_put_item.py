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

from tempest_lib import exceptions

from tempest.test import attr
from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase


class MagnetoDBPutItemTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBPutItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type=['PI-102', 'negative'])
    def test_put_item_in_table_with_wrong_name(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item("", item)

        exc = raises_cm.exception
        self.assertEqual(exc.body["message"],
                         "2 validation errors detected: Value '' at"
                         " 'tableName' failed to satisfy constraint:"
                         " Member must satisfy regular expression pattern:"
                         " [a-zA-Z0-9_.-]+; Value '' at 'tableName' failed"
                         " to satisfy constraint: Member must have length"
                         " greater than or equal to 3")
        self.assertIn("ValidationException", exc.body["__type"])

    @attr(type=['PI-103', 'negative'])
    def test_put_item_in_table_with_short_name(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item("qw", item)

        exc = raises_cm.exception
        self.assertEqual(exc.body["message"],
                         "1 validation error detected: Value 'qw' at"
                         " 'tableName' failed to satisfy constraint:"
                         " Member must have length greater than or equal to 3")
        self.assertIn("ValidationException", exc.body["__type"])

    @attr(type=['PI-104', 'negative'])
    def test_put_item_in_table_with_long_name(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        name_longer_than_255_characters = self.random_name(256)

        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(name_longer_than_255_characters, item)

        exc = raises_cm.exception
        self.assertEqual(exc.body["message"],
                         "1 validation error detected: Value" +
                         name_longer_than_255_characters +
                         "at 'tableName' failed to satisfy constraint:"
                         " Member must have length less than or equal to 255")
        self.assertIn("ValidationException", exc.body["__type"])
