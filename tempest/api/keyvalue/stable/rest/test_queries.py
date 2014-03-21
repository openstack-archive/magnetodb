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


class MagnetoDBQueriesTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBQueriesTest, self).setUp()
        self.tname = rand_name().replace('-', '')

    def test_query(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')

        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    def test_query_limit(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        items = self.populate_smoke_table(self.tname, 1, 10)

        key_conditions = {
            'forum': {
                'attribute_value_list': [items[0]['forum']],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }

        headers1, body1 = self.client.query(table_name=self.tname,
                                            key_conditions=key_conditions,
                                            limit=2,
                                            consistent_read=True)
        self.assertEqual(2, body1['count'])
        last = body1['last_evaluated_key']
        # query remaining records
        headers2, body2 = self.client.query(table_name=self.tname,
                                            key_conditions=key_conditions,
                                            exclusive_start_key=last,
                                            consistent_read=True)
        self.assertEqual(8, body2['count'])
        self.assertNotIn(body1['items'][0], body2['items'])
        self.assertNotIn(body1['items'][1], body2['items'])

    def test_query_no_consistent_read(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions={},
                                          consistent_read=False)
        self.assertEqual(body['items'][0], item)

    def test_query_only_hash_in_key_cond(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    def test_query_attributes_to_get_hash(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['forum']
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          attributes_to_get=attributes_to_get,
                                          consistent_read=True)
        self.assertEqual(len(body['items'][0]), 1)

    def test_query_attributes_to_get_hash_and_range(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['forum', 'subject']
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          attributes_to_get=attributes_to_get,
                                          consistent_read=True)
        self.assertEqual(len(body['items'][0]), 2)

    def test_query_attributes_to_get_non_key_atr(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['last_posted_by']
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          attributes_to_get=attributes_to_get,
                                          consistent_read=True)
        self.assertEqual(len(body['items'][0]), 1)

    def test_query_attributes_to_get_empty(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = []
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          attributes_to_get=attributes_to_get,
                                          consistent_read=True)
        self.assertEqual(len(body['items'][0]), 5)
