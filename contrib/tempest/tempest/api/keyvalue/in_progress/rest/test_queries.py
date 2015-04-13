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

from tempest_lib import exceptions

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest import test


class MagnetoDBQueriesTestCase(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBQueriesTestCase, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @test.attr(type=['Q-7', 'negative'])
    def test_query_empty_table_name(self):
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name='',
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-8', 'negative'])
    def test_query_too_short_table_name(self):
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name='qq',
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-9', 'negative'])
    def test_query_too_long_table_name(self):
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name='q' * 256,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-10_2'])
    def test_query_max_table_name(self):
        tname = 'q' * 255
        self._create_test_table(self.smoke_attrs, tname, self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(tname, 'forum1', 'subject2',
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
        headers, body = self.client.query(table_name=tname,
                                          consistent_read=True,
                                          key_conditions=key_conditions)
        self.assertTrue(body['count'] > 0)

    @test.attr(type=['Q-27', 'negative'])
    def test_query_attributes_to_get_empty(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Attribute list can not be empty", error_msg)

    def _query_key_cond_comparison_negative(self, attr_type, value,
                                            value_list, compare_op,
                                            second_key_cond='subject',
                                            result=exceptions.BadRequest):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': attr_type}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs, self.tname, schema,
                                wait_for_active=True)
        item = {
            "forum": {"N": '1'},
            "subject": {attr_type: value}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'N': '1'}],
                'comparison_operator': 'EQ'
            },
            second_key_cond: {
                'attribute_value_list': value_list,
                'comparison_operator': compare_op
            }
        }
        with self.assertRaises(result):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-11'])
    def test_query_table_name_query_upper_case(self):
        # NOTE(aostapenko) Check if table name is case sensitive in dynamodb
        self._create_test_table(self.smoke_attrs, self.tname,
                                self.smoke_schema, wait_for_active=True)
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
        self.tname = self.tname.upper()
        headers, body = self.client.query(table_name=self.tname,
                                          consistent_read=True,
                                          key_conditions=key_conditions)
        self.assertTrue(body['count'] > 0)

    @test.attr(type=['Q-64', 'negative'])
    def test_query_exclusive_incorrect(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        exclusive = {'forum': {'S': 'one'}, 'subject': {'S': 'two'}}
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              exclusive_start_key=exclusive,
                              consistent_read=True)

    @test.attr(type=['Q-72', 'negative'])
    def test_query_index_too_short_index_name(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        index_name = 'in'
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'last_posted_by': {
                'attribute_value_list': [{'S': 'John'}],
                'comparison_operator': 'EQ'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              index_name=index_name,
                              consistent_read=True)

    @test.attr(type=['Q-73', 'negative'])
    def test_query_index_too_long_index_name(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        index_name = 'i' * 256
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'last_posted_by': {
                'attribute_value_list': [{'S': 'John'}],
                'comparison_operator': 'EQ'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              index_name=index_name,
                              consistent_read=True)

    @test.attr(type=['Q-83'])
    def test_query_limit_0_items_3(self):
        # NOTE(aostapenko) Needs verification in dynamodb
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        items = self.populate_smoke_table(self.tname, 1, 3)

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

        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          limit=0,
                                          consistent_read=True)
        self.assertEqual(3, body['count'])
        self.assertNotIn('last_evaluated_key', body)

    @test.attr(type=['Q-94'])
    def test_query_select_all_projected(self):
        lsi = [
            {
                'index_name': 'last_posted_by_index',
                'key_schema': [
                    {'attribute_name': self.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'last_posted_by', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            }
        ]
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'last_posted_by': {
                'attribute_value_list': [{'S': 'John'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True,
                                          index_name='last_posted_by_index',
                                          select='ALL_PROJECTED_ATTRIBUTES')
        self.assertEqual(body['count'], 1)
        self.assertEqual(len(body['items'][0]), 3)
