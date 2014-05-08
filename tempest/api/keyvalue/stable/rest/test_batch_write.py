# Copyright 2014 Mirantis Inc.
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
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBBatchWriteTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBBatchWriteTest, self).setUp()
        self.tname = rand_name().replace('-', '')

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
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
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
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
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
        self.assertEqual(body['count'], 1)

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: [{'delete_request':
                                                        {'key': key}}]}}
        headers, body = self.client.batch_write_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
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
        self.assertEqual(body['count'], 1)

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: [{'delete_request':
                                                        {'key': key}}]}}
        headers, body = self.client.batch_write_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
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
        self.assertEqual(body['count'], 1)

        item = self.build_smoke_item('forum1', 'subject3',
                                     'message text', 'John', '10')

        key = {self.hashkey: {'S': 'forum1'}, self.rangekey: {'S': 'subject2'}}
        request_body = {'request_items': {self.tname: [{'delete_request':
                                                        {'key': key}},
                                                       {'put_request':
                                                        {'item': item}}]}}
        headers, body = self.client.batch_write_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
        headers, body = self.client.query(self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(1, body['count'])
        self.assertIn(item, body['items'])

    @attr(type=['BWI-51'])
    def test_batch_write_put_25_tables(self):
        tnames = [rand_name().replace('-', '') for i in range(0, 25)]
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
        headers, body = self.client.batch_write_item(request_body)
        self.assertIn('unprocessed_items', body)
        self.assertEqual(body['unprocessed_items'], {})
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

    @attr(type=['BWI-50'])
    def test_batch_write_correct_tname_and_put_request(self):
        self._create_test_table(self.build_x_attrs('S'),
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2')
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
