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


class MagnetoDBPutGetItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBPutGetItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_get_item_valid_name(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(self.table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(self.table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    def test_get_item_short_table_name(self):
        self.table_name = self.random_name(3)
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(self.table_name, 'forum1', 'subject2')

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(self.table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    def test_get_item_no_attributes_to_get(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(self.table_name, 'forum1', 'subject2')

        get_resp = self.client.get_item(self.table_name, key)
        self.assertEqual(get_resp[1]['item'], item)

    def test_get_item_no_match_key(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: {'S': 'no_match_key'},
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(self.table_name, 'forum1', 'subject2')

        get_resp = self.client.get_item(self.table_name, key)
        self.assertEqual(get_resp[1], {})

    def test_get_item_key_type_n(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{'attribute_name': 'key', 'attribute_type': 'N'}],
            self.table_name,
            [{'attribute_name': 'key', 'key_type': 'HASH'}])
        item = {
            'key': {'N': '111'},
            'last_posted_by': {'S': 'John'}
        }
        key = {'key': {'N': '111'}}
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name, key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

        get_resp = self.client.get_item(self.table_name, {'key': {'N': '11'}})
        self.assertEqual(get_resp[1], {})

    def test_get_item_key_type_s(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{'attribute_name': 'key', 'attribute_type': 'S'}],
            self.table_name,
            [{'attribute_name': 'key', 'key_type': 'HASH'}])
        item = {
            'key': {'S': 'sss'},
            'last_posted_by': {'S': 'John'}
        }
        key = {'key': {'S': 'sss'}}
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name, key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

        get_resp = self.client.get_item(self.table_name,
                                        {'key': {'S': 'ppp'}})
        self.assertEqual(get_resp[1], {})

    def test_get_item_key_type_b(self):
        self.table_name = rand_name().replace('-', '')
        self.client.create_table(
            [{'attribute_name': 'key', 'attribute_type': 'B'}],
            self.table_name,
            [{'attribute_name': 'key', 'key_type': 'HASH'}])
        item = {
            'key': {'B': 'blob'},
            'last_posted_by': {'S': 'John'}
        }
        key = {'key': {'B': 'blob'}}
        self.client.put_item(self.table_name, item)
        get_resp = self.client.get_item(self.table_name, key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

        get_resp = self.client.get_item(self.table_name,
                                        {'key': {'B': 'qwer'}})
        self.assertEqual(get_resp[1], {})
