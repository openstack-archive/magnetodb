# Copyright 2014 Mirantis Inc.
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

import base64

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest_lib import exceptions
from tempest.test import attr


class MagnetoDBBatchWriteTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBBatchWriteTest, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @attr(type=['BWI-1'])
    def test_batch_write_mandatory(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        headers, body = self.client.batch_write_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-2'])
    def test_batch_write_all_params(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        headers, body = self.client.batch_write_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-10'])
    def test_batch_write_put_n(self):
        self._create_test_table(self.build_x_attrs('N'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('N', '1000', '2000', ('message', 'N', '1001'))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        headers, body = self.client.batch_write_item(request_body)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'N': '1000'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-13'])
    def test_batch_write_put_n_ns(self):
        self._create_test_table(self.build_x_attrs('N'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('N', '1000', '2000',
                                 ('message', 'NS', ['1001', '1002']))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        headers, body = self.client.batch_write_item(request_body)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'N': '1000'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-36'])
    def test_batch_write_delete_s(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            self.rangekey: {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        if not body['count']:
            raise exceptions.TempestException("No item to delete.")

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: [{'delete_request':
                                                        {'key': key}}]}}
        self.client.batch_write_item(request_body)
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(0, body['count'])

    @attr(type=['BWI-53'])
    def test_batch_write_delete(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            self.rangekey: {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        if not body['count']:
            raise exceptions.TempestException("No item to delete.")

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: [{'delete_request':
                                                        {'key': key}}]}}
        self.client.batch_write_item(request_body)
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(0, body['count'])

    @attr(type=['BWI-40'])
    def test_batch_write_put_delete(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            self.rangekey: {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        if not body['count']:
            raise exceptions.TempestException("No item to delete.")

        item = self.build_smoke_item('forum1', 'subject3',
                                     'message text', 'John', '10')

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: [{'delete_request':
                                                        {'key': key}},
                                                       {'put_request':
                                                        {'item': item}}]}}
        self.client.batch_write_item(request_body)
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(1, body['count'])
        self.assertIn(item, body['items'])

    @attr(type=['BWI-51'])
    def test_batch_write_put_25_tables(self):
        tnames = [rand_name(self.table_prefix).replace('-', '')
                  for i in range(0, 25)]
        for tname in tnames:
            self._create_test_table(self.smoke_attrs,
                                    tname,
                                    self.smoke_schema,
                                    wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject3',
                                     'message text', 'John', '10')
        request_body = {'request_items': dict(
            (tname, [{'put_request': {'item': item}}]) for tname in tnames)}

        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            self.rangekey: {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        self.client.batch_write_item(request_body)
        for tname in tnames:
            headers, body = self.client.query(tname,
                                              key_conditions=key_conditions,
                                              consistent_read=True)
            self.assertEqual(1, body['count'])
            self.assertIn(item, body['items'])

    @attr(type=['BWI-16'])
    def test_batch_write_put_s(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        self.client.batch_write_item(request_body)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-14'])
    def test_batch_write_put_b(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        value = base64.b64encode('\xFF')
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'B', value))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        self.client.batch_write_item(request_body)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-15'])
    def test_batch_write_put_bs(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        value1 = base64.b64encode('\xFF\x01')
        value2 = base64.b64encode('\xFF\x02')
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'BS', [value1, value2]))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        self.client.batch_write_item(request_body)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-50'])
    def test_batch_write_correct_tname_and_put_request(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2')
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        self.client.batch_write_item(request_body)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(item, body['items'][0])

    @attr(type=['BWI-42', 'negative'])
    def test_batch_write_put_delete_same_item(self):
        self._create_test_table(self.smoke_attrs, self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        key = {
            self.hashkey: {'S': 'forum1'},
            self.rangekey: {'S': 'subject2'}
        }

        request_body = {
            'request_items': {self.tname: [
                {'put_request': {'item': item}},
                {'delete_request': {'key': key}}
            ]}
        }

        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.batch_write_item(request_body)

        exception = raises_cm.exception
        self.assertIn("ValidationError", exception._error_string)
        self.assertIn("More than one", exception._error_string)

    @attr(type=['BWI-54_3'])
    def test_batch_write_non_existent_table(self):
        tname = 'non_existent_table'
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {tname: [{'put_request':
                                                   {'item': item}}]}}

        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.batch_write_item(request_body)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Not Found", error_msg)
        self.assertIn("Table 'non_existent_table' does not exist", error_msg)

    @attr(type=['BWI-12'])
    def test_batch_write_put_empty_attr_name(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('', 'N', '1000'))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.batch_write_item(request_body)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Wrong attribute name '' found", error_msg)

    @attr(type=['negative'])
    def test_batch_write_two_tables_one_nonexistent(self):
        non_existent_table = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {
            self.tname: [{
                'put_request': {'item': item}}],
            non_existent_table: [{
                'put_request': {'item': item}}],
        }}
        with self.assertRaises(exceptions.NotFound) as raises_cm:
            self.client.batch_write_item(request_body)
        error_msg = raises_cm.exception._error_string
        self.assertIn("TableNotExistsException", error_msg)
        key_conditions = {
            self.hashkey: {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual([], body['items'])
