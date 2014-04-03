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


class MagnetoDBQueriesTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBQueriesTest, self).setUp()
        self.tname = rand_name().replace('-', '')

    def test_query_mandatory(self):
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
                                          key_conditions=key_conditions)
        self.assertIn('items', body)

    def test_query_min_table_name(self):
        tname = 'xxx'
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

    def test_query_consistent_read_false(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        item = self.put_smoke_item(self.tname, 'forum1', 'subject2',
                                   'message text', 'John', '10')
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions={},
                                          consistent_read=False)
        self.assertIn('items', body)
        if len(body['items']):
            self.assertEqual(body['items'][0], item)

    def test_query_only_hash_in_key_cond(self):
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

    def test_query_attributes_to_get_select_specific(self):
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
                                          consistent_read=True,
                                          select='SPECIFIC_ATTRIBUTES')
        self.assertEqual(len(body['items'][0]), 1)

    def test_query_attributes_to_get_totally_incorrect(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_attributes_to_get_partially_incorrect(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_key_cond_b(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'B'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_key_cond_n(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_key_cond_bn(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
        value = base64.b64encode('\xFF')
        item = {
            "forum": {"B": value},
            "subject": {"N": '1'}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'B': value}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'N': '1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    def test_query_key_cond_bs(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'B'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
        value = base64.b64encode('\xFF')
        item = {
            "forum": {"B": value},
            "subject": {"S": '1'}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'B': value}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': '1'}],
                'comparison_operator': 'EQ'
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertTrue(body['count'] > 0)

    def test_query_key_cond_ns(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
        item = {
            "forum": {"N": '1'},
            "subject": {"S": '1'}
        }
        self.client.put_item(self.tname, item)
        key_conditions = {
            'forum': {
                'attribute_value_list': [{'N': '1'}],
                'comparison_operator': 'EQ'
            },
            'subject': {
                'attribute_value_list': [{'S': '1'}],
                'comparison_operator': 'EQ'
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

    def test_query_limit_check_last_evaluated(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
        items = self.populate_smoke_table(self.tname, 1, 2)
        attributes_to_get = ['forum', 'subject']
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
                                          attributes_to_get=attributes_to_get,
                                          limit=1,
                                          consistent_read=True)
        last = body['last_evaluated_key']
        # query remaining records
        self.assertEqual(last, body['items'][0])

    def test_query_limit_10_items_5(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_limit_0_items_5(self):
        self.client.create_table(self.smoke_attrs,
                                 self.tname,
                                 self.smoke_schema)
        self.wait_for_table_active(self.tname)
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
                                          limit=0,
                                          consistent_read=True)
        self.assertEqual(5, body['count'])
        self.assertNotIn('last_evaluated_key', body)

    def test_query_one_key_cond_eq_n(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'N'},
            {'attribute_name': 'subject', 'attribute_type': 'N'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_one_key_cond_eq_s(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'S'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
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

    def test_query_one_key_cond_zero_return(self):
        attrs = [
            {'attribute_name': 'forum', 'attribute_type': 'S'},
            {'attribute_name': 'subject', 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'forum', 'key_type': 'HASH'},
            {'attribute_name': 'subject', 'key_type': 'RANGE'}
        ]
        self.client.create_table(attrs, self.tname, schema)
        self.wait_for_table_active(self.tname)
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

    def _query_key_cond_comparison(self, attr_type, value, compare_value,
                                   compare_op, result):
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
                'attribute_value_list': [{attr_type: compare_value}],
                'comparison_operator': compare_op
            }
        }
        headers, body = self.client.query(table_name=self.tname,
                                          key_conditions=key_conditions,
                                          consistent_read=True)
        self.assertEqual(body['count'], result)

    def test_query_one_key_cond_le_n_01(self):
        self._query_key_cond_comparison('N', '1', '2', 'LE', 1)

    def test_query_one_key_cond_le_n_02(self):
        self._query_key_cond_comparison('N', '1', '1', 'LE', 1)

    def test_query_one_key_cond_le_n_03(self):
        self._query_key_cond_comparison('N', '1', '0', 'LE', 0)

    def test_query_one_key_cond_lt_n_01(self):
        self._query_key_cond_comparison('N', '1', '2', 'LT', 1)

    def test_query_one_key_cond_lt_n_02(self):
        self._query_key_cond_comparison('N', '1', '1', 'LT', 0)

    def test_query_one_key_cond_lt_n_03(self):
        self._query_key_cond_comparison('N', '1', '0', 'LT', 0)

    def test_query_one_key_cond_ge_n_01(self):
        self._query_key_cond_comparison('N', '1', '2', 'GE', 0)

    def test_query_one_key_cond_ge_n_02(self):
        self._query_key_cond_comparison('N', '1', '1', 'GE', 1)

    def test_query_one_key_cond_ge_n_03(self):
        self._query_key_cond_comparison('N', '1', '0', 'GE', 1)

    def test_query_one_key_cond_gt_n_01(self):
        self._query_key_cond_comparison('N', '1', '2', 'GT', 0)

    def test_query_one_key_cond_gt_n_02(self):
        self._query_key_cond_comparison('N', '1', '1', 'GT', 0)

    def test_query_one_key_cond_gt_n_03(self):
        self._query_key_cond_comparison('N', '1', '0', 'GT', 1)

    def test_query_one_key_cond_le_s_01(self):
        self._query_key_cond_comparison('S', '1', '2', 'LE', 1)

    def test_query_one_key_cond_le_s_02(self):
        self._query_key_cond_comparison('S', '1', '1', 'LE', 1)

    def test_query_one_key_cond_le_s_03(self):
        self._query_key_cond_comparison('S', '1', '0', 'LE', 0)

    def test_query_one_key_cond_lt_s_01(self):
        self._query_key_cond_comparison('S', '1', '2', 'LT', 1)

    def test_query_one_key_cond_lt_s_02(self):
        self._query_key_cond_comparison('S', '1', '1', 'LT', 0)

    def test_query_one_key_cond_lt_s_03(self):
        self._query_key_cond_comparison('S', '1', '0', 'LT', 0)

    def test_query_one_key_cond_ge_s_01(self):
        self._query_key_cond_comparison('S', '1', '2', 'GE', 0)

    def test_query_one_key_cond_ge_s_02(self):
        self._query_key_cond_comparison('S', '1', '1', 'GE', 1)

    def test_query_one_key_cond_ge_s_03(self):
        self._query_key_cond_comparison('S', '1', '0', 'GE', 1)

    def test_query_one_key_cond_gt_s_01(self):
        self._query_key_cond_comparison('S', '1', '2', 'GT', 0)

    def test_query_one_key_cond_gt_s_02(self):
        self._query_key_cond_comparison('S', '1', '1', 'GT', 0)

    def test_query_one_key_cond_gt_s_03(self):
        self._query_key_cond_comparison('S', '1', '0', 'GT', 1)

    def test_query_one_key_cond_le_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, value2, 'LE', 1)

    def test_query_one_key_cond_le_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, value2, 'LE', 1)

    def test_query_one_key_cond_le_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, value2, 'LE', 0)

    def test_query_one_key_cond_lt_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, value2, 'LT', 1)

    def test_query_one_key_cond_lt_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, value2, 'LT', 0)

    def test_query_one_key_cond_lt_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, value2, 'LT', 0)

    def test_query_one_key_cond_ge_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, value2, 'GE', 0)

    def test_query_one_key_cond_ge_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, value2, 'GE', 1)

    def test_query_one_key_cond_ge_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, value2, 'GE', 1)

    def test_query_one_key_cond_gt_b_01(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFF')
        self._query_key_cond_comparison('B', value1, value2, 'GT', 0)

    def test_query_one_key_cond_gt_b_02(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFE')
        self._query_key_cond_comparison('B', value1, value2, 'GT', 0)

    def test_query_one_key_cond_gt_b_03(self):
        value1 = base64.b64encode('\xFE')
        value2 = base64.b64encode('\xFD')
        self._query_key_cond_comparison('B', value1, value2, 'GT', 1)
