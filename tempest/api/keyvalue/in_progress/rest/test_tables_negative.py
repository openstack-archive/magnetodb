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


class MagnetoDBTablesTestNegative(MagnetoDBTestCase):

    def test_duplicate_table(self):
        tname = rand_name().replace('-', '')
        self.client.create_table(self.smoke_attrs, tname,
                                 self.smoke_schema)
        self.assertRaises(exceptions.Duplicate,
                          self.client.create_table,
                          self.smoke_attrs, tname,
                          self.smoke_schema)

    def test_create_table_malformed_01(self):
        tname = rand_name().replace('-', '')
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=None,
                          table_name=tname,
                          schema=None)

    def test_create_table_malformed_02(self):
        tname = rand_name().replace('-', '')
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=self.smoke_attrs,
                          table_name=tname,
                          schema=None)

    def test_create_table_malformed_03(self):
        tname = rand_name().replace('-', '')
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=None,
                          table_name=tname,
                          schema=self.smoke_schema)

    def test_create_table_malformed_attrs_01(self):
        tname = rand_name().replace('-', '')
        attr_def = []
        key_schema = []
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=tname,
                          schema=key_schema)

    def test_create_table_malformed_attrs_02(self):
        tname = rand_name().replace('-', '')
        attr_def = [{'attribute_name': 'test'}]
        key_schema = [{'attribute_name': 'test', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=tname,
                          schema=key_schema)

    def test_create_table_malformed_attrs_03(self):
        tname = rand_name().replace('-', '')
        attr_def = [{'attribute_type': 'S'}]
        key_schema = [{'attribute_name': 'test', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=tname,
                          schema=key_schema)

    def test_create_table_hash_key_set(self):
        tname = rand_name().replace('-', '')
        attr_def = [{'attribute_name': 'set', 'attribute_type': 'SS'}]
        schema = [{'attribute_name': 'set', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=tname,
                          schema=schema)
