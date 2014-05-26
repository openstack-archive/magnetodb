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

import copy

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest import exceptions
from tempest import test


class MagnetoDBCreateTableNegativeTestCase(MagnetoDBTestCase):

    def __init__(self, *args, **kwargs):
        super(MagnetoDBCreateTableNegativeTestCase,
              self).__init__(*args, **kwargs)
        self.tname = rand_name().replace('-', '')

    @test.attr(type=['CreT-49', 'negative'])
    def test_create_table_empty_table_name(self):
        tname = ''
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    tname,
                                    self.schema_hash_only,
                                    self.smoke_lsi)

    @test.attr(type=['CreT-53_1', 'negative'])
    def test_create_table_index_without_name_param(self):
        lsi = copy.deepcopy(self.smoke_lsi)
        del lsi[0]['index_name']
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    lsi)

    @test.attr(type=['CreT-53_2', 'negative'])
    def test_create_table_index_without_schema_param(self):
        lsi = copy.deepcopy(self.smoke_lsi)
        del lsi[0]['key_schema']
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    lsi)

    @test.attr(type=['CreT-53_3', 'negative'])
    def test_create_table_index_without_projection_param(self):
        lsi = copy.deepcopy(self.smoke_lsi)
        del lsi[0]['projection']
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.smoke_attrs + self.index_attrs,
                                    self.tname,
                                    self.smoke_schema,
                                    lsi)

    @test.attr(type=['CreT-33', 'negative'])
    def test_create_table_invalid_schema_key_type(self):
        schema = [{'attribute_name': 'forum', 'key_type': 'INVALID'}]
        with self.assertRaises(exceptions.BadRequest):
            self._create_test_table(self.one_attr, self.tname, schema)
