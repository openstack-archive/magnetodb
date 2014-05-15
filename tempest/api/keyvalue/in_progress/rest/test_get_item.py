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

from tempest import exceptions
from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.test import attr


class MagnetoDBGetItemTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBGetItemTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type='GI-12')
    def test_get_item_long_table_name(self):
        table_name = self.random_name(255)
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

    @attr(type=['GI-13', 'negative'])
    def test_get_item_non_existent_table_name(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        attributes_to_get = ['last_posted_by']
        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.get_item("nonexistenttable",
                                 key,
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("Not Found",
                      exception._error_string)
        self.assertIn("The resource could not be found.",
                      exception._error_string)
        self.assertIn("Table 'nonexistenttable' does not exists",
                      exception._error_string)

    # TODO(ValidationException or simple NotFound)
    @attr(type=['GI-15', 'negative'])
    def test_get_item_two_symbol_table_name(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        attributes_to_get = ['last_posted_by']
        with self.assertRaises(exceptions.NotFound):
            self.client.get_item(self.random_name(2),
                                 key,
                                 attributes_to_get,
                                 True)

    # TODO(ValidationException or simple NotFound)
    @attr(type=['GI-16', 'negative'])
    def test_get_item_more_than_255_symbol_table_name(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        attributes_to_get = ['last_posted_by']
        with self.assertRaises(exceptions.NotFound):
            self.client.get_item(self.random_name(260),
                                 key,
                                 attributes_to_get,
                                 True)

    @attr(type=['GI-51', 'negative'])
    def test_get_item_resource_not_found_exception(self):
        item = self.build_smoke_item('forum1', 'subject2',
                                     last_posted_by='John')
        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        attributes_to_get = ['last_posted_by']
        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.get_item("nonexistenttable",
                                 key,
                                 attributes_to_get,
                                 True)
        exception = raises_cm.exception
        self.assertIn("ResourceNotFoundException", exception._error_string)
