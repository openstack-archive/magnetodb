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

import time

from tempest_lib import exceptions

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBListTableTestCase(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBListTableTestCase, self).setUp()
        self.tables = []

    def tearDown(self):
        i = 0
        while i < len(self.tables):
            tname = self.tables[i]
            try:
                self.client.delete_table(tname)
                i += 1
            except (exceptions.BadRequest, exceptions.NotFound):
                del self.tables[i]

        while self.tables:
            tname = self.tables[0]
            try:
                self.client.describe_table(tname)
            except exceptions.NotFound:
                del self.tables[0]
            time.sleep(1)
        super(MagnetoDBListTableTestCase, self).tearDown()

    def _create_n_tables(self, num):
        for i in range(0, num):
            tname = rand_name(self.table_prefix).replace('-', '')
            self._create_test_table(self.smoke_attrs, tname,
                                    self.smoke_schema,
                                    cleanup=False,
                                    wait_for_active=True)
            self.tables.append(tname)

    @attr(type=['LisT-15', 'negative'])
    def test_list_tables_limit_0(self):
        with self.assertRaises(exceptions.BadRequest):
            self.client.list_tables(limit=0)

    @attr(type=['LisT-35'])
    def test_list_tables_exclusive_255_symb(self):
        tnames = 'a' * 255, 'b' * 255
        for tname in tnames:
            self._create_test_table(self.smoke_attrs, tname,
                                    self.smoke_schema,
                                    cleanup=False,
                                    wait_for_active=True)
            self.tables.append(tname)
        last_evaluated_table_name = self.tables[0]
        headers, body = self.client.list_tables(
            exclusive_start_table_name=last_evaluated_table_name)
        self.assertEqual(len(body['tables']), 1)

    @attr(type=['LisT-36', 'negative'])
    def test_list_tables_exclusive_2_symb(self):
        with self.assertRaises(exceptions.BadRequest):
            self.client.list_tables(exclusive_start_table_name="aa")

    @attr(type=['LisT-37', 'negative'])
    def test_list_tables_exclusive_256_symb(self):
        with self.assertRaises(exceptions.BadRequest):
            self.client.list_tables(exclusive_start_table_name="a"*256)
