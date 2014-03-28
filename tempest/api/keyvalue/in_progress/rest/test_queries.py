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
