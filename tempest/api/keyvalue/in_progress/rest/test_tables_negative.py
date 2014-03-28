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

    def __init__(self, *args, **kwargs):
        super(MagnetoDBTablesTestNegative, self).__init__(*args, **kwargs)
        self.tname = rand_name().replace('-', '')

    def test_duplicate_table(self):
        self.client.create_table(self.smoke_attrs, self.tname,
                                 self.smoke_schema)
        self.assertRaises(exceptions.Duplicate,
                          self.client.create_table,
                          self.smoke_attrs, self.tname,
                          self.smoke_schema)

    def test_create_table_malformed_01(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=None,
                          table_name=self.tname,
                          schema=None)

    def test_create_table_malformed_02(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=self.smoke_attrs,
                          table_name=self.tname,
                          schema=None)

    def test_create_table_malformed_03(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=None,
                          table_name=self.tname,
                          schema=self.smoke_schema)

    def test_create_table_malformed_04(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=self.smoke_attrs,
                          table_name=None,
                          schema=self.smoke_schema)

    def test_create_table_malformed_05(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=None,
                          table_name=None,
                          schema=self.smoke_schema)

    def test_create_table_malformed_06(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=self.smoke_attrs,
                          table_name=None,
                          schema=None)

    def test_create_table_malformed_07(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=None,
                          table_name=None,
                          schema=None)

    def test_create_table_malformed_attrs_01(self):
        attr_def = []
        key_schema = []
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=self.tname,
                          schema=key_schema)

    def test_create_table_malformed_attrs_02(self):
        attr_def = [{'attribute_name': 'test'}]
        key_schema = [{'attribute_name': 'test', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=self.tname,
                          schema=key_schema)

    def test_create_table_malformed_attrs_03(self):
        attr_def = [{'attribute_type': 'S'}]
        key_schema = [{'attribute_name': 'test', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=self.tname,
                          schema=key_schema)

    def test_create_table_hash_key_set(self):
        attr_def = [{'attribute_name': 'set', 'attribute_type': 'SS'}]
        schema = [{'attribute_name': 'set', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=self.tname,
                          schema=schema)

    def test_create_table_only_range_key(self):
        attr_def = [{'attribute_name': 'forum', 'attribute_type': 'S'}]
        schema = [{'attribute_name': 'forum', 'key_type': 'RANGE'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=self.tname,
                          schema=schema)

    def test_create_table_empty_schema(self):
        attr_def = [{'attribute_name': 'forum', 'attribute_type': 'S'}]
        schema = []
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          attr_def=attr_def,
                          table_name=self.tname,
                          schema=schema)

    def test_create_table_non_index_attr(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs,
                          self.tname,
                          self.schema_hash_only)

    def test_create_table_non_index_attr_01(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          self.tname,
                          self.smoke_schema)

    def test_create_table_non_index_attr_02(self):
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          self.tname,
                          self.schema_hash_only,
                          self.smoke_lsi)

    def test_create_table_redundand_schema_hash(self):
        schema = [{'attribute_name': 'last_posted_by', 'key_type': 'HASH'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          self.tname,
                          self.smoke_schema + schema)

    def test_create_table_redundand_schema_range(self):
        schema = [{'attribute_name': 'last_posted_by', 'key_type': 'RANGE'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          self.tname,
                          self.smoke_schema + schema)

    def test_create_table_redundand_schema_other(self):
        schema = [{'attribute_name': 'last_posted_by'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          self.tname,
                          self.smoke_schema + schema)

    def test_create_table_invalid_schema_key_type(self):
        schema = [{'attribute_name': 'forum', 'key_type': 'INVALID'}]
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.one_attr,
                          self.tname,
                          schema)

    def test_create_table_too_short_name(self):
        tname = 'qq'
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          tname,
                          self.schema_hash_only,
                          self.smoke_lsi)

    def test_create_table_too_long_name(self):
        tname = 'q' * 256
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          tname,
                          self.schema_hash_only,
                          self.smoke_lsi)

    def test_create_table_empty_table_name(self):
        tname = ''
        self.assertRaises(exceptions.BadRequest,
                          self.client.create_table,
                          self.smoke_attrs + self.index_attrs,
                          tname,
                          self.schema_hash_only,
                          self.smoke_lsi)

    def test_create_table_too_long_attr_names(self):
        tname = rand_name().replace('-', '')
        attr_def = [
            {'attribute_name': 'f' * 256, 'attribute_type': 'S'},
            {'attribute_name': 's' * 256, 'attribute_type': 'S'}
        ]
        schema = [
            {'attribute_name': 'f' * 256, 'key_type': 'HASH'},
            {'attribute_name': 's' * 256, 'key_type': 'RANGE'}
        ]
        self.assertRaises(exceptions.BadRequest, self.client.create_table,
                          attr_def, tname, schema)

    def test_describe_table_nonexistent(self):
        tname = rand_name().replace('-', '')
        self.assertRaises(exceptions.NotFound, self.client.describe_table,
                          tname)

    def test_describe_table_empty_name(self):
        self.assertRaises(exceptions.BadRequest, self.client.describe_table,
                          table_name="")

    def test_describe_table_short_name(self):
        self.assertRaises(exceptions.BadRequest, self.client.describe_table,
                          table_name="aa")

    def test_describe_table_too_long_name(self):
        self.assertRaises(exceptions.BadRequest, self.client.describe_table,
                          table_name="a" * 256)
