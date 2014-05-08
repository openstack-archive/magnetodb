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
import binascii

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

    @attr(type='PI-3')
    def test_put_item_update_one_attribute(self):
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
                "exists": "true",
                "value": {"S": "Bob"}
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

    @attr(type='PI-4')
    def test_put_item_update_few_attributes(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
            "id": {"N": "1"}
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
            "id": {"N": "2"}
        }
        expected = {
            "author": {
                "exists": "true",
                "value": {"S": "Bob"}
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

    @attr(type='PI-5')
    def test_put_item_update_few_lines_without_exist_state(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
            "id": {"N": "1"}
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
            "id": {"N": "2"}
        }
        expected = {
            "author": {
                "value": {"S": "Bob"}
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

    @attr(type='PI-10')
    def test_put_item_with_few_attributes_of_type_b(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        blob = binascii.hexlify('fblob')
        item = {
            "message": {"B": "qazw"},
            "author": {"B": "qwer"},
            "blob": {"B": blob}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"B": "qazw"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["message"], {"B": "qazw"})
        self.assertEqual(get_resp[1]["item"]["author"], {"B": "qwer"})
        self.assertEqual(get_resp[1]['item']["blob"], {"B": blob})

    @attr(type='PI-11')
    def test_put_item_with_few_attributes_of_type_bs(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {"BS": ["qqqq", "wwww"]},
            "blob": {"BS": ["rrrr", "tttt"]}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(set(get_resp[1]["item"]["author"]["BS"]),
                         {"qqqq", "wwww"})
        self.assertEqual(set(get_resp[1]['item']["blob"]["BS"]),
                         {"rrrr", "tttt"})

    @attr(type='PI-15')
    def test_put_item_with_attributes_of_all_types(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {"SS": ["eeee", "qqqq", "nnnn"]},
            "id": {"N": "1"},
            "ids": {"NS": ["2", "3"]},
            "blob": {"B": "blob"},
            "blobs": {"BS": ["qqqq", "wwww", "eeee"]}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(set(get_resp[1]["item"]["author"]["SS"]),
                         {"eeee", "nnnn", "qqqq"})
        self.assertEqual(get_resp[1]["item"]["id"], {"N": "1"})
        self.assertEqual(set(get_resp[1]['item']["ids"]["NS"]),
                         {"2", "3"})
        self.assertEqual(get_resp[1]["item"]["blob"], {"B": "1"})
        self.assertEqual(set(get_resp[1]['item']["blobs"]["BS"]),
                         {"qqqq", "eeee", "wwww"})

    @attr(type='PI-50')
    def test_put_item_exist_state_by_default(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
            "id": {"N": "1"}
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
            "id": {"N": "2"}
        }
        expected = {
            "author": {
                "value": {"S": "Bob"}
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

    @attr(type='PI-51')
    def test_put_item_exists_true(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
            "id": {"N": "1"}
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Alice"},
            "id": {"N": "2"}
        }
        expected = {
            "author": {
                "exists": "true",
                "value": {"S": "Bob"}
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

    @attr(type=['PI-101', 'negative'])
    def test_put_item_in_nonexistent_table(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.put_item("nonexistenttable", item)

        exception = raises_cm.exception
        self.assertIn("Not Found", exception._error_string)
        self.assertIn("The resource could not be found.",
                      exception._error_string)
        self.assertIn("Table 'nonexistenttable' does not exist",
                      exception._error_string)

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
