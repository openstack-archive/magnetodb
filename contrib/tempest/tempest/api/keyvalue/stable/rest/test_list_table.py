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

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr
from tempest_lib import exceptions


class MagnetoDBListTableTestCase(MagnetoDBTestCase):

    force_tenant_isolation = True

    def setUp(self):
        super(MagnetoDBListTableTestCase, self).setUp()
        self.tables = []

    def tearDown(self):
        for tname in self.tables:
            try:
                self.client.delete_table(tname)
            except (exceptions.BadRequest, exceptions.NotFound):
                self.tables.remove(tname)
        while True:
            for tname in self.tables:
                try:
                    self.client.describe_table(tname)
                except exceptions.NotFound:
                    self.tables.remove(tname)
            if not self.tables:
                break
            time.sleep(1)
        super(MagnetoDBListTableTestCase, self).tearDown()

    @attr(type=['LisT-1'])
    def test_list_tables_empty(self):
        headers, body = self.client.list_tables()
        expected = {'tables': []}
        self.assertEqual(body, expected)

    @attr(type=['LisT-2'])
    def test_list_tables(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs, tname,
                                self.smoke_schema,
                                wait_for_active=True,
                                cleanup=False)
        self.tables.append(tname)
        headers, body = self.client.list_tables()
        url = self.client.base_url
        expected = {'tables': [{'href': '{url}/tables/{table}'.format(
            url=url, table=tname), 'rel': 'self'}]}
        self.assertEqual(body, expected)

    def _create_n_tables(self, num):
        for i in range(0, num):
            tname = rand_name(self.table_prefix).replace('-', '')
            self._create_test_table(self.smoke_attrs, tname,
                                    self.smoke_schema,
                                    cleanup=False,
                                    wait_for_active=True)
            self.tables.append(tname)

    @attr(type=['LisT-10'])
    def test_list_tables_no_limit_5_tables(self):
        tnum = 5
        self._create_n_tables(tnum)
        headers, body = self.client.list_tables()
        self.assertEqual(len(body['tables']), tnum)

    @attr(type=['LisT-12'])
    def test_list_tables_limit_3_10_tables_with_exclusive(self):
        tnum = 10
        limit = 3
        self._create_n_tables(tnum)
        last_evaluated_table_name = None
        url = self.client.base_url
        for i in range(0, tnum / limit):
            headers, body = self.client.list_tables(
                limit=limit,
                exclusive_start_table_name=last_evaluated_table_name)
            last_evaluated_table_name = body['last_evaluated_table_name']
            self.assertEqual(len(body['tables']), limit)
            self.assertEqual(body['tables'][-1]['href'],
                             '{url}/tables/{table}'.format(
                             url=url, table=last_evaluated_table_name))
        headers, body = self.client.list_tables(
            limit=limit,
            exclusive_start_table_name=last_evaluated_table_name)
        self.assertEqual(len(body['tables']), tnum % limit)

    @attr(type=['LisT-30'])
    def test_list_tables_no_exclusive(self):
        tnum = 5
        self._create_n_tables(tnum)
        headers, body = self.client.list_tables()
        self.assertEqual(len(body['tables']), tnum)

    @attr(type=['LisT-31', 'LisT-33'])
    def test_list_tables_exclusive(self):
        tnum = 5
        limit = 3
        self._create_n_tables(tnum)
        headers, body = self.client.list_tables(limit=limit)
        last_evaluated_table_name = body['last_evaluated_table_name']
        headers, body = self.client.list_tables(
            limit=limit,
            exclusive_start_table_name=last_evaluated_table_name)
        self.assertEqual(len(body['tables']), tnum % limit)

    @attr(type=['LisT-32'])
    def test_list_tables_exclusive_no_previous_run(self):
        tnum = 5
        self._create_n_tables(tnum)
        self.tables.sort()
        last_evaluated_table_name = self.tables[0]
        headers, body = self.client.list_tables(
            exclusive_start_table_name=last_evaluated_table_name)
        self.assertEqual(len(body['tables']), tnum - 1)

    @attr(type=['LisT-34'])
    def test_list_tables_exclusive_3_symb(self):
        tnames = 'aaa', 'bbb'
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

    @attr(type=['LisT-38'])
    def test_list_tables_exclusive_non_existent(self):
        tnames = 'aaa1', 'bbb'
        for tname in tnames:
            self._create_test_table(self.smoke_attrs, tname,
                                    self.smoke_schema,
                                    cleanup=False,
                                    wait_for_active=True)
            self.tables.append(tname)
        last_evaluated_table_name = 'aaa'
        headers, body = self.client.list_tables(
            exclusive_start_table_name=last_evaluated_table_name)
        self.assertEqual(len(body['tables']), 2)

    @attr(type=['LisT-16', 'negative'])
    def test_list_tables_limit_negative2(self):
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.list_tables(limit=-2)
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("'limit' property value[-2] is less then min_value",
                      error_msg)

    @attr(type=['LisT-17', 'negative'])
    def test_list_tables_limit_string(self):
        with self.assertRaises(exceptions.BadRequest) as raises_cm:
            self.client.list_tables(limit="str")
        error_msg = raises_cm.exception._error_string
        self.assertIn("Bad Request", error_msg)
        self.assertIn("Integer is expected", error_msg)
