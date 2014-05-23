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

    @attr(type='PI-52')
    def test_put_item_exists_false(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
            "id": {"N": "2"}
        }
        expected = {
            "id": {
                "exists": "false"
            }
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        put_resp = self.client.put_item(self.table_name,
                                        new_item, expected)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['author'],
                         {'S': 'Alice'})
        self.assertEqual(get_resp[1]['item']['id'],
                         {'N': '2'})

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

    @attr(type=['PI-114', 'negative'])
    def test_put_item_wrong_expected_section(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
        }
        expected = {
            "wrong_key": {
                "value": {"S": "Bob"}
            }
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, new_item, expected)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "The conditional request failed")
        self.assertIn("ConditionalCheckFailedException",
                      exception.body["__type"])

    @attr(type=['PI-111', 'negative'])
    def test_put_item_wrong_data_type_in_expected(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
        }
        expected = {
            "author": {
                "value": {"KK": "Bob"}
            }
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, new_item, expected)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "Supplied attribute_value is empty,"
                         " must contain exactly one of the "
                         "supported datatypes")
        self.assertIn("ValidationException", exception.body["__type"])

    @attr(type=['PI-113', 'negative'])
    def test_put_item_no_attribute_value(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {}
        }
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, item)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "Supplied attribute_value is empty,"
                         " must contain exactly one of the "
                         "supported datatypes")
        self.assertIn("ValidationException", exception.body["__type"])

    @attr(type=['PI-120', 'negative'])
    def test_put_item_conditional_check_failed(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
        }
        expected = {
            "author": {
                "value": {"S": "Dod"}
            }
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, new_item, expected)

        exception = raises_cm.exception
        self.assertEqual(exception.body["message"],
                         "The conditional request failed")
        self.assertIn("ConditionalCheckFailedException",
                      exception.body["__type"])

    @attr(type=['PI-123', 'negative'])
    def test_put_item_resource_not_found_exception(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.put_item("nonexistent_table", item)

        exception = raises_cm.exception
        self.assertIn("Not Found", exception._error_string)
        self.assertIn("The resource could not be found.",
                      exception._error_string)
        self.assertIn("Table 'nonexistenttable' does not exists",
                      exception._error_string)
