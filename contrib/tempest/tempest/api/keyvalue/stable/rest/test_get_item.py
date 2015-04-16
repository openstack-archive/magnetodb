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

from tempest_lib import exceptions
from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBGetItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBGetItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type='GI-1')
    def test_get_item_correct_mandatory_attributes(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        get_resp = self.client.get_item(table_name,
                                        key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})
        self.assertEqual(get_resp[1]['item']['message'], {'S': 'message_text'})
        self.assertEqual(get_resp[1]['item']['replies'], {'N': '1'})
        self.assertEqual(get_resp[1]['item']['forum'], {'S': 'forum1'})
        self.assertEqual(get_resp[1]['item']['subject'], {'S': 'subject2'})

    @attr(type='GI-2')
    def test_get_item_correct_mandatory_attributes_and_optional(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        attributes_to_get = ['last_posted_by', 'message']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        expected_response = {
            u'message': {
                u'S': u'message_text'
            },
            u'last_posted_by': {
                u'S': u'John'
            }
        }
        self.assertEqual(get_resp[1]['item'], expected_response)

    @attr(type='GI-10')
    def test_get_item_valid_name(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    @attr(type='GI-11')
    def test_get_item_short_table_name(self):
        table_name = self.random_name(3)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    # TODO(ValidationException or simple NotFound)
    @attr(type=['GI-14', 'negative'])
    def test_get_item_empty_table_name(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        attributes_to_get = ['last_posted_by']

        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.get_item("",
                                 key,
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("Not Found", exception._error_string)

    @attr(type='GI-20')
    def test_get_item_valid_attribute_to_get(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    @attr(type='GI-21')
    def test_get_item_no_attributes_to_get(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        get_resp = self.client.get_item(table_name, key)
        self.assertEqual(get_resp[1]['item'], item)

    @attr(type='GI-23')
    def test_get_item_attributes_to_get_none(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = []
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    @attr(type='GI-24')
    def test_get_item_more_attribute_to_get_than_exist(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['message', 'author']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item']['message'], {'S': 'message_text'})

    @attr(type='GI-25')
    def test_get_item_wrong_attribute_to_get(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['wrong_attribute']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)
        self.assertEqual(get_resp[1]['item'], {})

    @attr(type=['GI-26', 'negative'])
    def test_get_item_empty_names_of_attribute_to_get(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['', '']
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.get_item(table_name,
                                 key,
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)

    @attr(type='GI-30')
    def test_get_item_key_type_b(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'key', 'attribute_type': 'B'}],
            table_name,
            [{'attribute_name': 'key', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            'key': {'B': 'blob'},
            'last_posted_by': {'S': 'John'}
        }
        key = {'key': {'B': 'blob'}}
        self.client.put_item(table_name, item)
        get_resp = self.client.get_item(table_name, key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

        get_resp = self.client.get_item(table_name,
                                        {'key': {'B': 'qwer'}})
        self.assertEqual(get_resp[1], {})

    @attr(type='GI-31')
    def test_get_item_key_type_n(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'key', 'attribute_type': 'N'}],
            table_name,
            [{'attribute_name': 'key', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            'key': {'N': '111'},
            'last_posted_by': {'S': 'John'}
        }
        key = {'key': {'N': '111'}}
        self.client.put_item(table_name, item)
        get_resp = self.client.get_item(table_name, key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

        get_resp = self.client.get_item(table_name, {'key': {'N': '11'}})
        self.assertEqual(get_resp[1], {})

    @attr(type='GI-32')
    def test_get_item_key_type_s(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(
            [{'attribute_name': 'key', 'attribute_type': 'S'}],
            table_name,
            [{'attribute_name': 'key', 'key_type': 'HASH'}],
            wait_for_active=True)
        item = {
            'key': {'S': 'sss'},
            'last_posted_by': {'S': 'John'}
        }
        key = {'key': {'S': 'sss'}}
        self.client.put_item(table_name, item)
        get_resp = self.client.get_item(table_name, key)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

        get_resp = self.client.get_item(table_name,
                                        {'key': {'S': 'ppp'}})
        self.assertEqual(get_resp[1], {})

    @attr(type=['GI-33', 'negative'])
    def test_get_item_nonexistent_key_attribute(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['message']
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.get_item(table_name,
                                 None,
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)
        self.assertIn("Required property 'key' wasn't found or "
                      "it's value is null",
                      exception._error_string)

    @attr(type=['GI-34', 'negative'])
    def test_get_item_empty_key_attribute(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['message']
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.get_item(table_name,
                                 '',
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)

    @attr(type=['GI-35', 'negative'])
    def test_get_item_wrong_key_attribute(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['message']
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.get_item(table_name,
                                 {"message": {"K": "message"}},
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)

    @attr(type=['GI-???', 'negative'])
    def test_get_item_missing_range_key_attribute(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['message']
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.get_item(table_name,
                                 {self.hashkey: {"S": "forum1"}},
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)

    @attr(type=['GI-36', 'negative'])
    def test_get_item_no_match_key(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: {'S': 'no_match_key'},
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        get_resp = self.client.get_item(table_name, key)
        self.assertEqual(get_resp[1], {})

    # weak test for consistency
    @attr(type='GI-40')
    def test_get_item_consistent_read_true(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        get_resp = self.client.get_item(table_name,
                                        key,
                                        None,
                                        True)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    # weak test for consistency
    @attr(type='GI-41')
    def test_get_item_consistent_read_false(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        get_resp = self.client.get_item(table_name,
                                        key,
                                        None,
                                        False)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    # weak test for consistency
    @attr(type='GI-42')
    def test_get_item_consistent_read_none(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        put_resp = self.put_smoke_item(table_name, 'forum1', 'subject2')
        self.assertEqual(put_resp, item)

        get_resp = self.client.get_item(table_name,
                                        key,
                                        None,
                                        None)
        self.assertEqual(get_resp[1]['item']['last_posted_by'], {'S': 'John'})

    @attr(type=['GI-43', 'negative'])
    def test_get_item_wrong_consistency(self):
        table_name = self.random_name(10)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        self.put_smoke_item(table_name, 'forum1', 'subject2')

        attributes_to_get = ['message']

        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.get_item(table_name,
                                 key,
                                 attributes_to_get,
                                 "")
        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)
