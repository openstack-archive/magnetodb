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
import base64
import random
import string

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBDeleteItemTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBDeleteItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def random_name_cases(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length/2)).join(
                           random.choice(string.lowercase + string.digits)
                           for i in range((length + 1)/2))

    @attr(type='delIt-1')
    def test_delete_item_mandatory_fields_only(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)

        item = {
            "message": {"S": 'message_text'},
            "subject": {"S": 'testSubject'}
        }
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message":
                                               {"S": 'message_text'}})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-2')
    def test_delete_item_all_fields(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'messageId', 'attribute_type': 'S'},
             {'attribute_name': 'subject', 'attribute_type': 'S'},
             {'attribute_name': 'dateTime', 'attribute_type': 'N'},
             {'attribute_name': 'category', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'},
             {'attribute_name': 'messageId', 'key_type': 'RANGE'}],
            wait_for_active=True)

        item = {
            "message": {"S": 'message_text'},
            "messageId": {"S": '1'},
            "subject": {"S": 'testSubject'},
            "dateTime": {"N": '20140313164951'}
        }
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'},
                                         "messageId": {"S": '1'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        self.assertEqual(get_resp[1]['item']['messageId'],
                         {'S': '1'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message":
                                               {"S": 'message_text'},
                                               "messageId": {"S": '1'},
                                               "subject":
                                               {"S": 'testSubject'},
                                               "dateTime":
                                               {"N": '20140313164951'}}
                                              )
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'},
                                         "messageId": {"S": '1'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-10')
    def test_delete_item_one_table(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)

        item = {
            "message": {"S": 'message_text'}
        }
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message":
                                               {"S": 'message_text'}})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-12')
    def test_delete_item_short_name(self):
        self.table_name = self.random_name(3)
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'}
        }

        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message":
                                               {"S": 'message_text'}})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-19')
    def test_delete_item_mixed_case(self):
        self.table_name = self.random_name_cases(8).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'}
        }

        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message":
                                               {"S": 'message_text'}})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-30')
    def test_delete_item_type_b(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'B'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)

        value = base64.b64encode('\xFF')

        item = {
            "message": {"B": value}
        }
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"B": value}},
                                        consistent_read=True)
        self.assertEqual((get_resp[1]['item']['message']).get(
            'B').rstrip('\n'),             value)
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message": {
                                                  "B": value
                                              }})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"B": value}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-31')
    def test_delete_item_type_n(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'dateTime', 'attribute_type': 'N'},
             {'attribute_name': 'subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'},
             {'attribute_name': 'dateTime', 'key_type': 'RANGE'}],
            wait_for_active=True)

        item = {
            "message": {"S": 'message_text'},
            "subject": {"S": 'testSubject'},
            "dateTime": {"N": '20140313164951'}
        }
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'},
                                         "dateTime": {"N": '20140313164951'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message": {
                                                  "S": 'message_text'
                                              }, "dateTime": {
                                                  "N": '20140313164951'
                                              }})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'},
                                         "dateTime": {"N": '20140313164951'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})

    @attr(type='delIt-32')
    def test_delete_item_type_s(self):
        self.table_name = rand_name().replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'subject', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)

        item = {
            "message": {"S": 'message_text'},
            "subject": {"S": 'testSubject'},
        }
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': 'message_text'})
        delete_resp = self.client.delete_item(self.table_name,
                                              {"message": {
                                                  "S": 'message_text'
                                              }})
        self.assertEqual(delete_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1], {})
