# Copyright 2014 Symantec Corporation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from tempest_lib import exceptions

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBBatchGetTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBBatchGetTest, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @attr(type=['BGI-6'])
    def test_batch_get_table_name_255_symb(self):
        tname = 'x' * 255
        self._create_test_table(self.build_x_attrs('S'),
                                tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {tname: {'keys': [key]}}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn(tname, body['responses'])

    @attr(type=['BGI-7', 'negative'])
    def test_batch_get_table_name_2_symb(self):
        tname = 'xy'
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {tname: {'keys': [key]}}}
        with self.assertRaises(exceptions.BadRequest):
            self.client.batch_get_item(request_body)

    @attr(type=['BGI-8', 'negative'])
    def test_batch_get_table_name_256_symb(self):
        tname = 'x' * 256
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {tname: {'keys': [key]}}}
        with self.assertRaises(exceptions.BadRequest):
            self.client.batch_get_item(request_body)

    @attr(type=['BGI-92', 'negative'])
    def test_batch_get_101_items(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        items = []
        for i in range(0, 101):
            item = self.build_x_item('S', 'forum1', 'subject' + str(i),
                                     ('message', 'S', 'message text'))
            items.append(item)
            self.client.put_item(self.tname, item)
        keys = [{self.hashkey: {'S': 'forum1'},
                 self.rangekey: {'S': 'subject' + str(i)}}
                for i in range(0, 101)]
        request_body = {'request_items': {self.tname: {'keys': keys}}}
        with self.assertRaises(exceptions.BadRequest):
            self.client.batch_get_item(request_body)
