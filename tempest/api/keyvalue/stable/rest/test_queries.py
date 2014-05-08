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
from tempest import test


class MagnetoDBQueriesTestCase(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBQueriesTestCase, self).setUp()
        self.tname = rand_name().replace('-', '')

    @test.attr(type=['Q-1'])
    def test_query_mandatory(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
                                          key_conditions=key_conditions)
        self.assertIn('items', body)

    @test.attr(type=['Q-5'])
    def test_query_valid_name(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
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
        self.assertIn('items', body)
        self.assertIn(item, body['items'])

    @test.attr(type=['Q-14'])
    def test_query_attributes_to_get_not_specified(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
        self.assertIn('items', body)
        self.assertEqual(5, len(body['items'][0]))

    @test.attr(type=['Q-2'])
    def test_query_all_params(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
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
        attributes_to_get = ['forum', 'subject']

        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          attributes_to_get=attributes_to_get,
                                          select='SPECIFIC_ATTRIBUTES',
                                          scan_index_forward=True,
                                          index_name='last_posted_by_index',
                                          consistent_read=True
                                          )
        self.assertIn('items', body)
        result_items = body['items']
        self.assertEqual(1, len(result_items))
        result_item = result_items[0]
        self.assertEqual(2, len(result_item))
        for attribute in attributes_to_get:
            self.assertIn(attribute, result_item)
            self.assertEqual(item[attribute], result_item[attribute])

    @test.attr(type=['Q-10_1'])
    def test_query_min_table_name(self):
        tname = 'a01'
        self._create_test_table(self.smoke_attrs,
                                tname,
                                self.smoke_schema,
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

    @test.attr(type=['Q-46'])
    def test_query_consistent_read_false(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
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
                                          consistent_read=False)
        self.assertIn('items', body)
        if len(body['items']):
            self.assertEqual(body['items'][0], item)

    @test.attr(type=['Q-44'])
    def test_query_consistent_read_not_specified(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
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
                                          key_conditions=key_conditions)
        self.assertIn('items', body)
        if len(body['items']):
            self.assertEqual(body['items'][0], item)

    @test.attr(type=['Q-43'])
    def test_query_consistent_read_true(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
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
                                          consistent_read=False)
        self.assertIn('items', body)
        self.assertEqual(body['items'][0], item)

    @test.attr(type=['Q-13'])
    def test_query_attributes_to_get_hash_and_range(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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

    @test.attr(type=['Q-15'])
    def test_query_attributes_to_get_more_than_in_attr_def(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['forum', 'subject', 'last_posted_by', 'message',
                             'replies', 'attr1', 'attr2']
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

    @test.attr(type=['Q-18', 'Q-96'])
    def test_query_attributes_to_get_select_specific(self):
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
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          attributes_to_get=attributes_to_get,
                                          consistent_read=True,
                                          select='SPECIFIC_ATTRIBUTES')
        self.assertEqual(len(body['items'][0]), 1)

    @test.attr(type=['Q-25'])
    def test_query_attributes_to_get_totally_non_existent_fields(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['incorrect1', 'incorrect2', 'incorrect3']
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
        self.assertEqual(len(body['items'][0]), 0)

    @test.attr(type=['Q-26'])
    def test_query_attributes_to_get_partially_incorrect(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        attributes_to_get = ['forum', 'subject', 'incorrect3']
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

    @test.attr(type=['Q-63'])
    def test_query_exclusive_start_key_not_specified(self):
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
                                          consistent_read=True)
        self.assertEqual(3, body['count'])

    @test.attr(type=['Q-81'])
    def test_query_limit_10_items_5(self):
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

        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          limit=10,
                                          consistent_read=True)
        self.assertEqual(5, body['count'])
        self.assertNotIn('last_evaluated_key', body)

    @test.attr(type=['Q-87'])
    def test_query_scan_index_true(self):
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

        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          scan_index_forward=True,
                                          consistent_read=True)
        self.assertEqual(5, body['count'])
        self.assertEqual(items, body['items'])

    @test.attr(type=['Q-88'])
    def test_query_scan_index_false(self):
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

        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          scan_index_forward=False,
                                          consistent_read=True)
        items.reverse()
        self.assertEqual(5, body['count'])
        self.assertEqual(items, body['items'])

    @test.attr(type=['Q-82'])
    def test_query_limit_5_items_3(self):
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
                                          limit=5,
                                          consistent_read=True)
        self.assertEqual(3, body['count'])
        self.assertNotIn('last_evaluated_key', body)

    @test.attr(type=['Q-80'])
    def test_query_limit_5_items_11(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        items = self.populate_smoke_table(self.tname, 1, 11)

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
                                          limit=5,
                                          consistent_read=True)
        self.assertEqual(5, body['count'])
        last_item = body['items'][4]
        for k, v in body['last_evaluated_key'].iteritems():
            self.assertIn(k, last_item)
            self.assertEqual(v, last_item[k])

    @test.attr(type=['Q-102_1'])
    def test_query_one_key_cond_eq_n(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs, self.tname, schema,
                                wait_for_active=True)
        item = {
            "forum": {"N": '1'},
            "subject": {"N": '1'}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'N': '1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    @test.attr(type=['Q-102_2'])
    def test_query_one_key_cond_eq_s(self):
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
                'attribute_value_list': [{'S': '1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    @test.attr(type=['Q-102_3'])
    def test_query_one_key_cond_eq_b(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'B'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self._create_test_table(attrs,
                                self.tname,
                                schema,
                                wait_for_active=True)
        value = base64.b64encode('\xFF')
        item = {
            "forum": {"B": value},
            "subject": {"B": value}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'B': value}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    @test.attr(type=['Q-107'])
    def test_query_one_key_cond_zero_return(self):
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
                'attribute_value_list': [{'S': '2'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] == 0)

    def _query_key_cond_comparison(self, attr_type, value, compare_values,
                                   compare_op, result):
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
            'subject': {
                'attribute_value_list': compare_values,
                'comparison_operator': compare_op
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(body['count'], result)

    @test.attr(type=['Q-109_1'])
    def test_query_one_key_cond_le_n_01(self):
        self._query_key_cond_comparison('N', '1', [{'N': '2'}], 'LE', 1)

    @test.attr(type=['Q-109_2'])
    def test_query_one_key_cond_le_n_02(self):
        self._query_key_cond_comparison('N', '1', [{'N': '1'}], 'LE', 1)

    @test.attr(type=['Q-109_3'])
    def test_query_one_key_cond_le_n_03(self):
        self._query_key_cond_comparison('N', '1', [{'N': '0'}], 'LE', 0)

    @test.attr(type=['Q-112_1'])
    def test_query_one_key_cond_lt_n_01(self):
        self._query_key_cond_comparison('N', '1', [{'N': '2'}], 'LT', 1)

    @test.attr(type=['Q-112_2'])
    def test_query_one_key_cond_lt_n_02(self):
        self._query_key_cond_comparison('N', '1', [{'N': '1'}], 'LT', 0)

    @test.attr(type=['Q-112_3'])
    def test_query_one_key_cond_lt_n_03(self):
        self._query_key_cond_comparison('N', '1', [{'N': '0'}], 'LT', 0)

    @test.attr(type=['Q-115_1'])
    def test_query_one_key_cond_ge_n_01(self):
        self._query_key_cond_comparison('N', '1', [{'N': '2'}], 'GE', 0)

    @test.attr(type=['Q-115_2'])
    def test_query_one_key_cond_ge_n_02(self):
        self._query_key_cond_comparison('N', '1', [{'N': '1'}], 'GE', 1)

    @test.attr(type=['Q-115_3'])
    def test_query_one_key_cond_ge_n_03(self):
        self._query_key_cond_comparison('N', '1', [{'N': '0'}], 'GE', 1)

    @test.attr(type=['Q-118_1'])
    def test_query_one_key_cond_gt_n_01(self):
        self._query_key_cond_comparison('N', '1', [{'N': '2'}], 'GT', 0)

    @test.attr(type=['Q-118_2'])
    def test_query_one_key_cond_gt_n_02(self):
        self._query_key_cond_comparison('N', '1', [{'N': '1'}], 'GT', 0)

    @test.attr(type=['Q-118_3'])
    def test_query_one_key_cond_gt_n_03(self):
        self._query_key_cond_comparison('N', '1', [{'N': '0'}], 'GT', 1)

    @test.attr(type=['Q-109_4'])
    def test_query_one_key_cond_le_s_01(self):
        self._query_key_cond_comparison('S', '1', [{'S': '2'}], 'LE', 1)

    @test.attr(type=['Q-109_5'])
    def test_query_one_key_cond_le_s_02(self):
        self._query_key_cond_comparison('S', '1', [{'S': '1'}], 'LE', 1)

    @test.attr(type=['Q-109_6'])
    def test_query_one_key_cond_le_s_03(self):
        self._query_key_cond_comparison('S', '1', [{'S': '0'}], 'LE', 0)

    @test.attr(type=['Q-112_4'])
    def test_query_one_key_cond_lt_s_01(self):
        self._query_key_cond_comparison('S', '1', [{'S': '2'}], 'LT', 1)

    @test.attr(type=['Q-112_5'])
    def test_query_one_key_cond_lt_s_02(self):
        self._query_key_cond_comparison('S', '1', [{'S': '1'}], 'LT', 0)

    @test.attr(type=['Q-112_6'])
    def test_query_one_key_cond_lt_s_03(self):
        self._query_key_cond_comparison('S', '1', [{'S': '0'}], 'LT', 0)

    @test.attr(type=['Q-115_4'])
    def test_query_one_key_cond_ge_s_01(self):
        self._query_key_cond_comparison('S', '1', [{'S': '2'}], 'GE', 0)

    @test.attr(type=['Q-115_5'])
    def test_query_one_key_cond_ge_s_02(self):
        self._query_key_cond_comparison('S', '1', [{'S': '1'}], 'GE', 1)

    @test.attr(type=['Q-115_6'])
    def test_query_one_key_cond_ge_s_03(self):
        self._query_key_cond_comparison('S', '1', [{'S': '0'}], 'GE', 1)

    @test.attr(type=['Q-118_4'])
    def test_query_one_key_cond_gt_s_01(self):
        self._query_key_cond_comparison('S', '1', [{'S': '2'}], 'GT', 0)

    @test.attr(type=['Q-118_5'])
    def test_query_one_key_cond_gt_s_02(self):
        self._query_key_cond_comparison('S', '1', [{'S': '1'}], 'GT', 0)

    @test.attr(type=['Q-118_6'])
    def test_query_one_key_cond_gt_s_03(self):
        self._query_key_cond_comparison('S', '1', [{'S': '0'}], 'GT', 1)

    @test.attr(type=['Q-109_7'])
    def test_query_one_key_cond_le_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'LE', 1)

    @test.attr(type=['Q-109_8'])
    def test_query_one_key_cond_le_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'LE', 1)

    @test.attr(type=['Q-109_9'])
    def test_query_one_key_cond_le_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'LE', 0)

    @test.attr(type=['Q-112_7'])
    def test_query_one_key_cond_lt_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'LT', 1)

    @test.attr(type=['Q-112_8'])
    def test_query_one_key_cond_lt_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'LT', 0)

    @test.attr(type=['Q-112_9'])
    def test_query_one_key_cond_lt_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'LT', 0)

    @test.attr(type=['Q-115_7'])
    def test_query_one_key_cond_ge_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'GE', 0)

    @test.attr(type=['Q-115_8'])
    def test_query_one_key_cond_ge_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'GE', 1)

    @test.attr(type=['Q-115_9'])
    def test_query_one_key_cond_ge_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'GE', 1)

    @test.attr(type=['Q-118_7'])
    def test_query_one_key_cond_gt_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'GT', 0)

    @test.attr(type=['Q-118_8'])
    def test_query_one_key_cond_gt_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'GT', 0)

    @test.attr(type=['Q-118_9'])
    def test_query_one_key_cond_gt_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, [{'B': value2}], 'GT', 1)

    @test.attr(type=['Q-121_1'])
    def test_query_one_key_cond_begins_with_s(self):
        self._query_key_cond_comparison('S', 'startend', [{'S': 'start'}],
                                        'BEGINS_WITH', 1)

    @test.attr(type=['Q-121_2'])
    def test_query_one_key_cond_begins_with_b(self):
        value1 = base64.b64encode('\x00')
        value2 = base64.b64encode('\x00')
        self._query_key_cond_comparison('B', value1, [{'B': value2}],
                                        'BEGINS_WITH', 1)

    @test.attr(type=['Q-125_1'])
    def test_query_one_key_cond_between_n_01(self):
        self._query_key_cond_comparison('N', '1', [{'N': '0'}, {'N': '2'}],
                                        'BETWEEN', 1)

    @test.attr(type=['Q-125_2'])
    def test_query_one_key_cond_between_n_02(self):
        self._query_key_cond_comparison('N', '1', [{'N': '1'}, {'N': '2'}],
                                        'BETWEEN', 1)

    @test.attr(type=['Q-125_3'])
    def test_query_one_key_cond_between_n_03(self):
        self._query_key_cond_comparison('N', '1', [{'N': '1'}, {'N': '1'}],
                                        'BETWEEN', 1)

    @test.attr(type=['Q-125_4'])
    def test_query_one_key_cond_between_n_04(self):
        self._query_key_cond_comparison('N', '1', [{'N': '2'}, {'N': '3'}],
                                        'BETWEEN', 0)

    @test.attr(type=['Q-125_5'])
    def test_query_one_key_cond_between_n_05(self):
        self._query_key_cond_comparison('N', '1', [{'N': '-1'}, {'N': '0'}],
                                        'BETWEEN', 0)

    @test.attr(type=['Q-67'])
    def test_query_index_correct(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        index_name = 'last_posted_by_index'
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
                                          index_name=index_name,
                                          consistent_read=True)
        self.assertEqual(len(body['items']), 1)

    @test.attr(type=['Q-71'])
    def test_query_index_not_specified(self):
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                self.tname,
                                self.smoke_schema,
                                self.smoke_lsi,
                                wait_for_active=True)
        self.put_smoke_item(self.tname, 'forum1', 'subject2',
                            'message text', 'John', '10')
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'S': 'forum1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': 'subject2'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(len(body['items']), 1)

    @test.attr(type=['Q-93'])
    def test_query_select_all(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
                                          consistent_read=True,
                                          select='ALL_ATTRIBUTES')
        self.assertEqual(body['count'], 1)
        self.assertEqual(len(body['items'][0]), 5)

    @test.attr(type=['Q-95'])
    def test_query_select_count(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
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
                                          consistent_read=True,
                                          select='COUNT')
        self.assertEqual(body['count'], 1)
        self.assertNotIn('items', body)
