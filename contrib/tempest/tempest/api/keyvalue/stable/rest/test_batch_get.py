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

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBBatchGetTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBBatchGetTest, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @attr(type=['BGI-1'])
    def test_batch_get_mandatory(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: {'keys': [key]}}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn('unprocessed_keys', body)
        self.assertIn('responses', body)
        expected_responses = {self.tname: [item]}
        self.assertEqual({}, body['unprocessed_keys'])
        self.assertEqual(expected_responses, body['responses'])

    @attr(type=['BGI-2'])
    def test_batch_get_all_params(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        attr_to_get = [self.hashkey]
        request_body = {
            'request_items':
            {
                self.tname:
                {
                    'keys': [key],
                    'attributes_to_get': attr_to_get,
                    'consistent_read': True
                }
            }
        }
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn('unprocessed_keys', body)
        self.assertIn('responses', body)
        expected_responses = {self.tname: [{'forum': {'S': 'forum1'}}]}
        self.assertEqual({}, body['unprocessed_keys'])
        self.assertEqual(expected_responses, body['responses'])

    @attr(type=['BGI-3'])
    def test_batch_get_one_table(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: {'keys': [key]}}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertIn(self.tname, body['responses'])

    @attr(type=['BGI-4'])
    def test_batch_get_several_tables(self):
        tables = []
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        for i in range(0, 3):
            tname = rand_name(self.table_prefix).replace('-', '')
            tables.append(tname)
            self._create_test_table(self.build_x_attrs('S'),
                                    tname,
                                    self.smoke_schema,
                                    wait_for_active=True)
            self.client.put_item(tname, item)

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {tname:
                        {'keys': [key]} for tname in tables}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertEqual(set(tables), set(body['responses'].keys()))

    @attr(type=['BGI-5'])
    def test_batch_get_table_name_3_symb(self):
        tname = 'xyz'
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

    @attr(type=['BGI-90'])
    def test_batch_get_99_items(self):
        self._batch_get_n_items(99)

    @attr(type=['BGI-91'])
    def test_batch_get_100_items(self):
        self._batch_get_n_items(100)

    def _batch_get_n_items(self, items_count):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        items = []
        for i in range(0, items_count):
            item = self.build_x_item('S', 'forum1', 'subject' + str(i),
                                     ('message', 'S', 'message text'))
            items.append(item)
            self.client.put_item(self.tname, item)
        keys = [{self.hashkey: {'S': 'forum1'},
                 self.rangekey: {'S': 'subject' + str(i)}}
                for i in range(0, items_count)]
        request_body = {'request_items': {self.tname: {'keys': keys}}}
        headers, body = self.client.batch_get_item(request_body)
        self.assertEqual(items_count, len(body['responses'][self.tname]))

    @attr(type=['BGI-20'])
    def test_batch_get_no_attr_to_get(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: {'keys': [key]}}}
        headers, body = self.client.batch_get_item(request_body)
        response_item = body['responses'][self.tname][0]
        for attribute in item.keys():
            self.assertIn(attribute, response_item)

    @attr(type=['BGI-21'])
    def test_batch_get_attr_to_get_all(self):
        attr_to_get = [self.hashkey, self.rangekey, 'message']
        self._batch_get_attr_to_get(attr_to_get)

    @attr(type=['BGI-22'])
    def test_batch_get_attr_to_get_some(self):
        attr_to_get = [self.rangekey, 'message']
        self._batch_get_attr_to_get(attr_to_get)

    def _batch_get_attr_to_get(self, attr_to_get):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {
            'request_items':
            {
                self.tname:
                {
                    'keys': [key],
                    'attributes_to_get': attr_to_get
                }
            }
        }
        headers, body = self.client.batch_get_item(request_body)
        response_item = body['responses'][self.tname][0]
        for attribute in attr_to_get:
            self.assertIn(attribute, response_item)

    @attr(type=['BGI-51'])
    def test_batch_get_consistent_read_true(self):
        self._batch_get_consistent_read(True)

    @attr(type=['BGI-52'])
    def test_batch_get_consistent_read_false(self):
        self._batch_get_consistent_read(False)

    def _batch_get_consistent_read(self, consistent_read):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        self.client.put_item(self.tname, item)
        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {
            'request_items':
            {
                self.tname:
                {
                    'keys': [key],
                    'consistent_read': consistent_read
                }
            }
        }
        headers, body = self.client.batch_get_item(request_body)
        if consistent_read:
            self.assertIn(item, body['responses'][self.tname])
