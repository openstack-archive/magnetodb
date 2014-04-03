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
from tempest import exceptions
from tempest.test import attr


class MagnetoDBQueriesTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBQueriesTest, self).setUp()
        self.tname = rand_name().replace('-', '')

    @attr(type='negative')
    def test_query_with_empty_key_cond(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions={},
                          consistent_read=True)

    @attr(type='negative')
    def test_query_without_key_cond(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          consistent_read=True)

    @attr(type='negative')
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
        self.assertRaises(exceptions.NotFound,
                          self.client.query,
                          table_name='non_existent_table',
                          key_conditions=key_conditions,
                          consistent_read=True)

    @attr(type='negative')
    def test_query_only_range_in_key_cond(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        key_conditions = {
            'subject': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          consistent_read=True)

    @attr(type='negative')
    def test_query_non_key_attr_in_key_cond(self):
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.build_smoke_item('forum1', 'subject2',
                                     'message text', 'John', '10')
        self.client.put_item(self.tname, item)
        key_conditions = {
            'last_posted_by': {
                'attribute_value_list': [{'S': 'subject'}],
                'comparison_operator': 'BEGINS_WITH'
            }
        }
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          consistent_read=True)

    @attr(type='negative')
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name='',
                          key_conditions=key_conditions,
                          consistent_read=True)

    @attr(type='negative')
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name='qq',
                          key_conditions=key_conditions,
                          consistent_read=True)

    @attr(type='negative')
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name='q' * 256,
                          key_conditions=key_conditions,
                          consistent_read=True)

    def test_query_max_table_name(self):
        tname = 'q' * 255
        self.client.create_table(self.smoke_attrs, tname, self.smoke_schema)
        self.wait_for_table_active(tname)
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

    def test_query_upper_case_table_name(self):
        tname = 'table_name'
        self.client.create_table(self.smoke_attrs,
                                 tname,
                                 self.smoke_schema)
        self.wait_for_table_active(tname)
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
        headers, body = self.client.query(table_name=tname.upper(),
                                          consistent_read=True,
                                          key_conditions=key_conditions)
        self.assertTrue(body['count'] > 0)

    @attr(type='negative')
    def test_query_attributes_to_get_select_all(self):
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          attributes_to_get=attributes_to_get,
                          consistent_read=True,
                          select='ALL_ATTRIBUTES')

    @attr(type='negative')
    def test_query_attributes_to_get_select_all_projected(self):
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          attributes_to_get=attributes_to_get,
                          consistent_read=True,
                          select='ALL_PROJECTED_ATTRIBUTES')

    @attr(type='negative')
    def test_query_attributes_to_get_select_count(self):
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          attributes_to_get=attributes_to_get,
                          consistent_read=True,
                          select='COUNT')

    @attr(type='negative')
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
        self.assertRaises(exceptions.BadRequest,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          attributes_to_get=attributes_to_get,
                          consistent_read=True)

    def _query_key_cond_comparison_negative(self, attr_type, value,
                                            value_list, compare_op,
                                            result=exceptions.BadRequest):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': attr_type}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
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
            'subject': {
                'attribute_value_list': value_list,
                'comparison_operator': compare_op
            }
        }
        self.assertRaises(result,
                          self.client.query,
                          table_name=self.tname,
                          key_conditions=key_conditions,
                          consistent_read=True)

    @attr(type='negative')
    def test_query_key_cond_invalid_comparison(self):
        self._query_key_cond_comparison_negative('N', '1', [{'N': '1'}], 'QQ')

    @attr(type='negative')
    def test_query_key_cond_lt_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'LT')

    @attr(type='negative')
    def test_query_key_cond_le_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'LE')

    @attr(type='negative')
    def test_query_key_cond_ge_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'GE')

    @attr(type='negative')
    def test_query_key_cond_gt_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'GT')

    @attr(type='negative')
    def test_query_key_cond_eq_two_attrs(self):
        self._query_key_cond_comparison_negative('N', '1',
                                                 [{'N': '1'}, {'N': '2'}],
                                                 'EQ')
