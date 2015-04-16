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

import base64
import random
import string

from tempest_lib import exceptions

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBPutItemTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBPutItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type='PI-1')
    def test_put_item_insert_one_attribute(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
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

    @attr(type='PI-2')
    def test_put_item_insert_few_attributes(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'author', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
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

    @attr(type='PI-3')
    def test_put_item_update_one_attribute(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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

    @attr(type='PI-5')
    def test_put_item_update_few_lines_without_exist_state(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "B"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        blob = base64.b64encode('fblob')
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
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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

    @attr(type='PI-12')
    def test_put_item_with_few_attributes_of_type_n(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "N"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
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

    @attr(type='PI-13')
    def test_put_item_with_few_attributes_of_type_ns(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
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

    @attr(type='PI-14')
    def test_put_item_with_few_attributes_of_type_ss(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
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

    def test_put_item_with_few_attributes_of_type_ssm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "SSM": {"eeee": "rrrr", "qqqq": "ttttt", "nnnn": "yyyy"}
            },
            "other": {"SSM": {"rrrr": "uuuu", "tttt": "gggg"}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["SSM"],
                         {"eeee": "rrrr", "qqqq": "ttttt", "nnnn": "yyyy"})
        self.assertEqual(get_resp[1]['item']["other"]["SSM"],
                         {"rrrr": "uuuu", "tttt": "gggg"})

    def test_put_item_with_few_attributes_of_type_snm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "SNM": {"eeee": 1, "qqqq": "3123.123", "nnnn": 345}
            },
            "other": {"SNM": {"rrrr": 123, "tttt": "33.13223"}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["SNM"],
                         {"eeee": '1', "qqqq": "3123.123", "nnnn": '345'})
        self.assertEqual(get_resp[1]['item']["other"]["SNM"],
                         {"rrrr": '123', "tttt": "33.13223"})

    def test_put_item_with_few_attributes_of_type_sbm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "SBM": {"eeee": base64.b64encode("\xFF\xFE"),
                        "qqqq": base64.b64encode("\xFE\xFE"),
                        "nnnn": base64.b64encode("\xEF\xFE")}
            },
            "other": {"SBM": {"rrrr": base64.b64encode("\xFF\xFE"),
                              "tttt": base64.b64encode("\xFE\xFE")}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["SBM"],
                         {"eeee": base64.b64encode("\xFF\xFE"),
                          "qqqq": base64.b64encode("\xFE\xFE"),
                          "nnnn": base64.b64encode("\xEF\xFE")})
        self.assertEqual(get_resp[1]['item']["other"]["SBM"],
                         {"rrrr": base64.b64encode("\xFF\xFE"),
                          "tttt": base64.b64encode("\xFE\xFE")})

    def test_put_item_with_few_attributes_of_type_nsm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "NSM": {1: "rrrr", "234.234": "ttttt", 2: "yyyy"}
            },
            "other":  {"NSM": {2: "uuuu", "23": "gggg"}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["NSM"],
                         {"1": "rrrr", "234.234": "ttttt", "2": "yyyy"})
        self.assertEqual(get_resp[1]['item']["other"]["NSM"],
                         {"2": "uuuu", "23": "gggg"})

    def test_put_item_with_few_attributes_of_type_nnm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "NNM": {"345": 1, "345.345": "3123.123", 546: 345}
            },
            "other": {"NNM": {"45.2": 123, 234: "33.13223"}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["NNM"],
                         {"345": '1', "345.345": "3123.123", '546': '345'})
        self.assertEqual(get_resp[1]['item']["other"]["NNM"],
                         {"45.2": '123', '234': "33.13223"})

    def test_put_item_with_few_attributes_of_type_nbm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "NBM": {"56": base64.b64encode("\xFF\xFE"),
                        546: base64.b64encode("\xFE\xFE"),
                        "456.567": base64.b64encode("\xEF\xFE")}
            },
            "other": {"NBM": {"567.567": base64.b64encode("\xFF\xFE"),
                              546: base64.b64encode("\xFE\xFE")}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["NBM"],
                         {"56": base64.b64encode("\xFF\xFE"),
                          "546": base64.b64encode("\xFE\xFE"),
                          "456.567": base64.b64encode("\xEF\xFE")})
        self.assertEqual(get_resp[1]['item']["other"]["NBM"],
                         {"567.567": base64.b64encode("\xFF\xFE"),
                          "546": base64.b64encode("\xFE\xFE")})

    def test_put_item_with_few_attributes_of_type_bsm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "BSM": {base64.b64encode("\xAA\xBA"): "rrrr",
                        base64.b64encode("\xAF\xBA"): "ttttt",
                        base64.b64encode("\xBA\xFA"): "yyyy"}
            },
            "other": {"BSM": {base64.b64encode("\xAF\xFA"): "uuuu",
                              base64.b64encode("\xAD\xDA"): "gggg"}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["BSM"],
                         {base64.b64encode("\xAA\xBA"): "rrrr",
                          base64.b64encode("\xAF\xBA"): "ttttt",
                          base64.b64encode("\xBA\xFA"): "yyyy"})
        self.assertEqual(get_resp[1]['item']["other"]["BSM"],
                         {base64.b64encode("\xAF\xFA"): "uuuu",
                          base64.b64encode("\xAD\xDA"): "gggg"})

    def test_put_item_with_few_attributes_of_type_bnm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "BNM": {base64.b64encode("\xBA\xBB"): 1,
                        base64.b64encode("\xAA\xBF"): "3123.123",
                        base64.b64encode("\xDA\xBD"): 345}
            },
            "other": {"BNM": {base64.b64encode("\xAA\xBA"): 123,
                              base64.b64encode("\xAA\xBA"): "33.13223"}}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"]["BNM"],
                         {base64.b64encode("\xBA\xBB"): "1",
                          base64.b64encode("\xAA\xBF"): "3123.123",
                          base64.b64encode("\xDA\xBD"): "345"})
        self.assertEqual(get_resp[1]['item']["other"]["BNM"],
                         {base64.b64encode("\xAA\xBA"): "123",
                          base64.b64encode("\xAA\xBA"): "33.13223"})

    def test_put_item_with_few_attributes_of_type_bbm(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{"attribute_name": "message", "attribute_type": "S"}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message"},
            "author": {
                "BBM": {
                    base64.b64encode("\xAF\xFE"): base64.b64encode("\xFF\xFE"),
                    base64.b64encode("\xDF\xDE"): base64.b64encode("\xFE\xFE"),
                    base64.b64encode("\xFF\xFD"): base64.b64encode("\xEF\xFE")
                }
            },
            "other": {
                "BBM": {
                    base64.b64encode("\xDF\xDE"): base64.b64encode("\xFF\xFE"),
                    base64.b64encode("\xDF\xDE"): base64.b64encode("\xFE\xFE")
                }
            }
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message"}},
                                        consistent_read=True)
        self.assertEqual(
            get_resp[1]["item"]["author"]["BBM"],
            {
                base64.b64encode("\xAF\xFE"): base64.b64encode("\xFF\xFE"),
                base64.b64encode("\xDF\xDE"): base64.b64encode("\xFE\xFE"),
                base64.b64encode("\xFF\xFD"): base64.b64encode("\xEF\xFE")
            }
        )
        self.assertEqual(
            get_resp[1]['item']["other"]["BBM"],
            {
                base64.b64encode("\xDF\xDE"): base64.b64encode("\xFF\xFE"),
                base64.b64encode("\xDF\xDE"): base64.b64encode("\xFE\xFE")
            }
        )

    @attr(type='PI-15')
    def test_put_item_with_attributes_of_all_types(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.assertEqual(get_resp[1]["item"]["blob"], {"B": "blob"})
        self.assertEqual(set(get_resp[1]['item']["blobs"]["BS"]),
                         {"qqqq", "eeee", "wwww"})

    @attr(type='PI-50')
    def test_put_item_exist_state_by_default(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
                "exists": True,
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
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
                "exists": False
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

    @attr(type='PI-80')
    def test_put_item_with_returned_none(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.client.put_item(self.table_name, item)
        put_resp = self.client.put_item(self.table_name,
                                        new_item,
                                        None,
                                        0,
                                        "NONE")
        self.assertEqual(put_resp[1], {})

    @attr(type=['PI-85', 'negative'])
    def test_put_item_with_returned_empty_string(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, item, None, 0, "")

        exception = raises_cm.exception
        self.assertIn("Bad request", exception._error_string)
        self.assertIn("ValidationError", exception._error_string)
        self.assertIn('u"Return values type \'\' isn\'t allowed',
                      exception._error_string)

    @attr(type=['PI-86', 'negative'])
    def test_put_item_with_wrong_string_in_returned_attribute(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name,
                                 item,
                                 None,
                                 0,
                                 "wrong_string")

        exception = raises_cm.exception
        self.assertIn("Bad request", exception._error_string)
        self.assertIn("ValidationError", exception._error_string)
        self.assertIn('u"Return values type \'wrong_string\' isn\'t allowed',
                      exception._error_string)

    @attr(type='PI-87')
    def test_put_item_with_returned_is_empty(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.client.put_item(self.table_name, item)
        put_resp = self.client.put_item(self.table_name,
                                        new_item,
                                        None,
                                        0,
                                        None)
        self.assertEqual(put_resp[1], {})

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

    @attr(type='PI-110')
    def test_put_item_with_existent_key(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"SS": ["Alice", "Bob"]},
            "other": {"SS": ["qqqq", "wwww"]}
        }
        new_item = {
            "message": {"S": "message_text"},
            "author": {"S": "Kris"},
            "other": {"S": "zzzz"}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})

        put_resp = self.client.put_item(self.table_name, new_item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message_text"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["author"], {"S": "Kris"})
        self.assertEqual(get_resp[1]['item']["other"], {"S": "zzzz"})

    @attr(type=['PI-111', 'negative'])
    def test_put_item_wrong_data_type_in_expected(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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

        exception_str = str(raises_cm.exception)
        self.assertIn("u'message': u\"Attribute type 'KK' is not recognized\"",
                      exception_str)
        self.assertIn("u'type': u'ValidationError'", exception_str)

    @attr(type='PI-112')
    def test_put_item_duplicate_key_name(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": "message_text_1"},
            "message": {"S": "message_text_2"}
        }
        put_resp = self.client.put_item(self.table_name, item)

        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": "message_text_2"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]["item"]["message"],
                         {"S": "message_text_2"})

    @attr(type=['PI-113', 'negative'])
    def test_put_item_no_attribute_value(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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

        exception_str = str(raises_cm.exception)
        self.assertIn("u'message': u\"Can't recognize attribute typed value "
                      "format: '{}'\"",
                      exception_str)
        self.assertIn("u'type': u'ValidationError'", exception_str)

    @attr(type=['PI-114', 'negative'])
    def test_put_item_wrong_expected_section(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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

        exception_str = str(raises_cm.exception)
        self.assertIn("u'message': u'The conditional request failed'",
                      exception_str)
        self.assertIn("u'type': u'ConditionalCheckFailedException'",
                      exception_str)

    @attr(type=['PI-120', 'negative'])
    def test_put_item_conditional_check_failed(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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

        exception_str = str(raises_cm.exception)
        self.assertIn("u'message': u'The conditional request failed'",
                      exception_str)
        self.assertIn("u'type': u'ConditionalCheckFailedException'",
                      exception_str)

    @attr(type=['PI-123', 'negative'])
    def test_put_item_resource_not_found_exception(self):
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"}
        }
        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.put_item("nonexistent_table", item)

        exception_str = str(raises_cm.exception)
        self.assertIn('"title":"Not Found"', exception_str)
        self.assertIn('"explanation":"The resource could not be found."',
                      exception_str)
        self.assertIn('"message":"Table \'nonexistent_table\' does not exist"',
                      exception_str)

    @attr(type=['PI-undef', 'negative'])
    def test_put_item_if_not_exists_negative(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
            "message": {
                "exists": False
            }
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})

        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, new_item, expected)

        exception = raises_cm.exception
        self.assertIn("ConditionalCheckFailedException", str(exception))

        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['author'],
                         {'S': 'Bob'})

    @attr(type='PI-undef2')
    def test_put_item_if_not_exists(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
        }

        expected = {
            "message": {
                "exists": False
            }
        }
        put_resp = self.client.put_item(self.table_name, item, expected)
        self.assertEqual(put_resp[1], {})

        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": 'message_text'}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['author'],
                         {'S': 'Bob'})

    @attr(type='PI-undef3')
    def test_put_item_if_not_exists_extra_condition(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "author": {"S": "Bob"},
        }

        expected = {
            "message": {
                "exists": False
            },
            "author": {
                "value": {"S": "Bob"}
            }
        }
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.put_item(self.table_name, item, expected)

        exception = raises_cm.exception
        self.assertIn(("Both expected_condition_map and "
                       "if_not_exist specified"), str(exception))

    def test_put_item_predefined_attr_replace(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [
                {'attribute_name': 'hash_attr', 'attribute_type': 'S'},
                {'attribute_name': 'range_attr', 'attribute_type': 'S'},
                {'attribute_name': 'extra_attr1', 'attribute_type': 'S'},
                {'attribute_name': 'extra_attr2', 'attribute_type': 'S'},
            ],
            self.table_name,
            [
                {'attribute_name': 'hash_attr', 'key_type': 'HASH'},
                {'attribute_name': 'range_attr', 'key_type': 'RANGE'},
            ],
            wait_for_active=True)
        item1 = {
            "hash_attr": {"S": "hash_value"},
            "range_attr": {"S": "range_value"},
            "extra_attr1": {"S": "extra_value1"},
            "extra_attr2": {"S": "extra_value2"},
        }

        put_resp = self.client.put_item(self.table_name, item1)
        self.assertEqual(put_resp[1], {})

        get_resp = self.client.get_item(self.table_name,
                                        {"hash_attr": {"S": "hash_value"},
                                         "range_attr": {"S": "range_value"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item'], item1)

        item2 = {
            "hash_attr": {"S": "hash_value"},
            "range_attr": {"S": "range_value"},
            "extra_attr1": {"S": "extra_value1"},
        }

        put_resp = self.client.put_item(self.table_name, item2)
        self.assertEqual(put_resp[1], {})

        get_resp = self.client.get_item(self.table_name,
                                        {"hash_attr": {"S": "hash_value"},
                                         "range_attr": {"S": "range_value"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item'], item2)

        item3 = {
            "hash_attr": {"S": "hash_value"},
            "range_attr": {"S": "range_value"},
            "extra_attr2": {"S": "extra_value2"},
        }

        put_resp = self.client.put_item(self.table_name, item3)
        self.assertEqual(put_resp[1], {})

        get_resp = self.client.get_item(self.table_name,
                                        {"hash_attr": {"S": "hash_value"},
                                         "range_attr": {"S": "range_value"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item'], item3)

        item4 = {
            "hash_attr": {"S": "hash_value"},
            "range_attr": {"S": "range_value"},
        }

        put_resp = self.client.put_item(self.table_name, item4)
        self.assertEqual(put_resp[1], {})

        get_resp = self.client.get_item(self.table_name,
                                        {"hash_attr": {"S": "hash_value"},
                                         "range_attr": {"S": "range_value"}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item'], item4)

    @attr(type='PI-81')
    def test_put_item_with_returned_all_old(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
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
        self.assertIn('attributes', put_resp[1])
        self.assertEqual(item, put_resp[1]["attributes"])

    def test_put_item_with_returned_all_old_with_indexes_01(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        lsi_definition = [
            {
                'index_name': 'time_index',
                'key_schema': [
                    {'attribute_name': 'message', 'key_type': 'HASH'},
                    {'attribute_name': 'time', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            }
        ]
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'thread', 'attribute_type': 'S'},
             {'attribute_name': 'time', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'},
             {'attribute_name': 'thread', 'key_type': 'RANGE'}],
            lsi_definition,
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "thread": {"S": 'thread_name'},
            "time": {"S": '200901010000'},
            "authors": {"SS": ["Alice", "Bob"]},
            "others": {"SS": ["qqqq", "wwww"]}
        }
        new_item = {
            "message": {"S": 'message_text'},
            "thread": {"S": 'thread_name'},
            "time": {"S": '201001010000'},
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
        self.assertIn('attributes', put_resp[1])
        self.assertEqual(item, put_resp[1]["attributes"])

    def test_put_item_with_returned_all_old_with_indexes_02(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        lsi_definition = [
            {
                'index_name': 'time_index',
                'key_schema': [
                    {'attribute_name': 'message', 'key_type': 'HASH'},
                    {'attribute_name': 'time', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            }
        ]
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'},
             {'attribute_name': 'thread', 'attribute_type': 'S'},
             {'attribute_name': 'time', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'},
             {'attribute_name': 'thread', 'key_type': 'RANGE'}],
            lsi_definition,
            wait_for_active=True)
        item = {
            "message": {"S": 'message_text'},
            "thread": {"S": 'thread_name'},
            "time": {"S": '200901010000'},
            "authors": {"SS": ["Alice", "Bob"]},
            "others": {"SS": ["qqqq", "wwww"]}
        }
        new_item = {
            "message": {"S": 'message_text'},
            "thread": {"S": 'thread_name'},
            "time": {"S": '201001010000'},
            "authors": {"SS": ["Kris", "Rob"]},
            "others": {"SS": ["xxxx", "zzzz"]}
        }
        put_resp = self.client.put_item(self.table_name,
                                        item,
                                        None,
                                        0)
        self.assertEqual(put_resp[1], {})
        put_resp = self.client.put_item(self.table_name,
                                        new_item,
                                        None,
                                        0)
        self.assertEqual(put_resp[1], {})
        put_resp = self.client.put_item(self.table_name,
                                        item,
                                        None,
                                        0,
                                        "ALL_OLD")
        self.assertIn('attributes', put_resp[1])
        self.assertEqual(new_item, put_resp[1]["attributes"])

    def test_put_item_unicode(self):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'message', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'message', 'key_type': 'HASH'}],
            wait_for_active=True)
        value = u'\u00c3\u00a2'
        item = {
            "message": {"S": value}
        }
        put_resp = self.client.put_item(self.table_name, item)
        self.assertEqual(put_resp[1], {})
        get_resp = self.client.get_item(self.table_name,
                                        {"message": {"S": value}},
                                        consistent_read=True)
        self.assertEqual(get_resp[1]['item']['message'],
                         {'S': value})
