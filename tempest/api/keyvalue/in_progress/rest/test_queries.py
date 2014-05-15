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

import base64

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest import exceptions
from tempest import test


class MagnetoDBQueriesTestCase(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBQueriesTestCase, self).setUp()
        self.tname = rand_name().replace('-', '')

    @test.attr(type='negative')
    def test_query_with_empty_key_cond(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions={},
                              consistent_read=True)

    @test.attr(type=['Q-39', 'negative'])
    def test_query_without_key_cond(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname, consistent_read=True)

    @test.attr(type=['Q-6', 'negative'])
    def test_query_non_existent_table(self):
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
        with self.assertRaises(exceptions.NotFound):
            self.client.query(table_name='non_existent_table',
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type='negative')
    def test_query_only_range_in_key_cond(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        key_conditions = {
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type='negative')
    def test_query_non_key_attr_in_key_cond(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        key_conditions = {
            'last_posted_by': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

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

    @test.attr(type=['Q-19', 'negative'])
    def test_query_attributes_to_get_select_all(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True,
                              select='ALL_ATTRIBUTES')

    @test.attr(type=['Q-20', 'negative'])
    def test_query_attributes_to_get_select_all_projected(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True,
                              select='ALL_PROJECTED_ATTRIBUTES')

    @test.attr(type=['Q-21', 'negative'])
    def test_query_attributes_to_get_select_count(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True,
                              select='COUNT')

    @test.attr(type=['Q-22', 'negative'])
    def test_query_attributes_to_get_select_empty(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True,
                              select='')

    @test.attr(type=['Q-23', 'negative'])
    def test_query_attributes_to_get_select_incorrect(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True,
                              select='INVALID')

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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True)

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

    @test.attr(type=['Q-108', 'negative'])
    def test_query_key_cond_invalid_comparison(self):
        self._query_key_cond_comparison_negative('N', '1', [{'N': '1'}], '%%')

    @test.attr(type=['Q-131', 'negative'])
    def test_query_key_cond_comparison_other_string(self):
        self._query_key_cond_comparison_negative('N', '1', [{'N': '1'}], 'QQ')

    @test.attr(type=['Q-129', 'negative'])
    def test_query_key_cond_empty_comparison(self):
        self._query_key_cond_comparison_negative('N', '1', [{'N': '1'}], '')

    @test.attr(type=['Q-113', 'negative'])
    def test_query_key_cond_lt_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'LT')

    @test.attr(type=['Q-114', 'negative'])
    def test_query_key_cond_lt_set(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'NS': ['1', '2']}], 'LT')

    @test.attr(type=['Q-110', 'negative'])
    def test_query_key_cond_le_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'LE')

    @test.attr(type=['Q-111', 'negative'])
    def test_query_key_cond_le_set(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'NS': ['1', '2']}], 'LE')

    @test.attr(type=['Q-116', 'negative'])
    def test_query_key_cond_ge_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'GE')

    @test.attr(type=['Q-117', 'negative'])
    def test_query_key_cond_ge_set(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'NS': ['1', '2']}], 'GE')

    @test.attr(type=['Q-119', 'negative'])
    def test_query_key_cond_gt_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'GT')

    @test.attr(type=['Q-123', 'negative'])
    def test_query_key_cond_begins_with_two_attrs(self):
        self._query_key_cond_comparison_negative('S', 'startend',
                                                 [{'S': 'start'},
                                                  {'S': 'end'}], 'BEGINS_WITH')

    @test.attr(type=['Q-124', 'negative'])
    def test_query_key_cond_begins_with_set(self):
        self._query_key_cond_comparison_negative('S', 'startend',
                                                 [{'SS': ['start', 'end']}],
                                                 'BEGINS_WITH')

    @test.attr(type=['Q-120', 'negative'])
    def test_query_key_cond_gt_set(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'NS': ['1', '2']}], 'GT')

    @test.attr(type=['Q-122', 'negative'])
    def test_query_key_cond_begins_with_bad_field(self):
        self._query_key_cond_comparison_negative('S', 'startend',
                                                 [{'S': 'start'}],
                                                 'BEGINS_WITH',
                                                 'non_existent_key_attr')

    @test.attr(type=['Q-126', 'negative'])
    def test_query_key_cond_between_bad_field(self):
        self._query_key_cond_comparison_negative('S', '1',
                                                 [{'S': '0'}, {'S': '2'}],
                                                 'BETWEEN',
                                                 'non_existent_key_attr')

    @test.attr(type=['Q-127', 'negative'])
    def test_query_key_cond_between_one_attr(self):
        self._query_key_cond_comparison_negative('S', '1', [{'S': '1'}],
                                                 'BETWEEN')

    @test.attr(type=['Q-128', 'negative'])
    def test_query_key_cond_between_set(self):
        self._query_key_cond_comparison_negative('S', '1',
                                                 [{'SS': ['0', '2']}],
                                                 'BETWEEN')

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

    @test.attr(type=['Q-16', 'negative'])
    def test_query_attributes_to_get_empty_values(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['']
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
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True)

    @test.attr(type=['Q-40_1', 'negative'])
    def test_query_key_cond_invalid_type_ss(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'SS': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-40_2', 'negative'])
    def test_query_key_cond_invalid_type_other(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'INVALID': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-45', 'negative'])
    def test_query_consistent_read_invalid_value(self):
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read='INVALID')

    @test.attr(type=['Q-62', 'negative'])
    def test_query_exclusive_set(self):
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
        exclusive = {'forum': {'SS': ['one', 'two']}}
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              exclusive_start_key=exclusive,
                              consistent_read=True)

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

    @test.attr(type=['Q-69', 'negative'])
    def test_query_index_non_existent(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        index_name = 'non_existent_index'
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
        with self.assertRaises(exceptions.NotFound):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              index_name=index_name,
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

    @test.attr(type=['Q-74', 'negative'])
    def test_query_index_empty_index_name(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        index_name = ''
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

    @test.attr(type=['Q-89', 'negative'])
    def test_query_scan_index_other(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        items = self.populate_smoke_table(self.tname, 1, 5)

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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              scan_index_forward=False,
                              consistent_read='INVALID')

    @test.attr(type=['Q-90', 'negative'])
    def test_query_scan_index_empty(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        items = self.populate_smoke_table(self.tname, 1, 5)

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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              scan_index_forward=False,
                              consistent_read='')

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

    @test.attr(type=['Q-98', 'negative'])
    def test_query_select_empty(self):
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True,
                              select='')

    @test.attr(type=['Q-99', 'negative'])
    def test_query_select_invalid(self):
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
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True,
                              select='INVALID')

    @test.attr(type=['Q-103'])
    def test_query_one_key_cond_eq_ss(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'S'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs, self.tname, schema,
                                wait_for_active=True)
        item = {
            "forum": {"S": '1'},
            "subject": {"S": '1'}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'SS': ['1', '2']}],
                'comparison_operator': 'EQ'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-104'])
    def test_query_one_key_cond_eq_s_n(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'S'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs, self.tname, schema,
                                wait_for_active=True)
        item = {
            "forum": {"S": '1'},
            "subject": {"S": '1'}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': '1'}, {'N': '1'}],
                'comparison_operator': 'EQ'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-105', 'negative'])
    def test_query_one_key_cond_b_eq_s(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'B'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs, self.tname, schema,
                                wait_for_active=True)
        value = base64.b64encode('\xFF')
        item = {
            "forum": {"B": value},
            "subject": {"B": value}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': '1'}],
                'comparison_operator': 'EQ'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-106', 'negative'])
    def test_query_one_key_cond_incorrect_key(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'B'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs, self.tname, schema,
                                wait_for_active=True)
        value = base64.b64encode('\xFF')
        item = {
            "forum": {"B": value},
            "subject": {"B": value}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'incorrect': {
                'attribute_value_list': [{'B': value}],
                'comparison_operator': 'EQ'
            }
        }
        with self.assertRaises(exceptions.BadRequest):
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              consistent_read=True)

    @test.attr(type=['Q-16'])
    def test_query_attributes_to_get_incorrect_attr_name(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['%$&']
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
            self.client.query(table_name=self.tname,
                              key_conditions=key_conditions,
                              attributes_to_get=attributes_to_get,
                              consistent_read=True)
