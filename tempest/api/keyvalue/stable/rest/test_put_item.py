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
from tempest.common.utils.data_utils import rand_name


class MagnetoDBPutItemTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBPutItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_put_item_insert_one_attribute(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}])
        item = {
            "message": {"S": 'message_text'}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})

    def test_put_item_insert_few_attributes(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'author', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}])
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['author'],
                         {'S': 'Bob'})

    def test_put_item_with_few_attributes_of_type_n(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{"attribute_name": "message", "attribute_type": "N"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}])
        item = {
            "message": {"N": "1"},
            "author": {"N": "2"},
            "blob": {"N": "3"}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"N": "1"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["message"], {"N": "1"})
        self.assertEqual(get_resp[1]["item"]["author"], {"N": "2"})
        self.assertEqual(get_resp[1]['item']["blob"], {"N": "3"})

    def test_put_item_with_few_attributes_of_type_ns(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}])
        item = {
            "message": {"S": "message"},
            "id": {"NS": ["1", "2"]},
            "other": {"NS": ["3", "4"]}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(set(get_resp[1]["item"]["id"]["NS"]),
                         {"1", "2"})
        self.assertEqual(set(get_resp[1]['item']["other"]["NS"]),
                         {"3", "4"})

    def test_put_item_with_few_attributes_of_type_ss(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}])
        item = {
            "message": {"S": "message"},
            "author": {"SS": ["eeee", "qqqq", "nnnn"]},
            "other": {"SS": ["rrrr", "tttt"]}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(set(get_resp[1]["item"]["author"]["SS"]),
                         {"eeee", "nnnn", "qqqq"})
        self.assertEqual(set(get_resp[1]['item']["other"]["SS"]),
                         {"rrrr", "tttt"})

    def test_put_item_with_returned_none(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}])
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
        self.client.put_item(self.table_name, item)
        put_resp = self.client.put_item(self.table_name,
                                        new_item,
                                        None,
                                        0,
                                        "NONE")
        self.assertEqual(put_resp[1], {})
