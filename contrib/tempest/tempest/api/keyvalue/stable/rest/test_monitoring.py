# Copyright 2014 Symantec Corporation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import random
import string

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name


class MagnetoDBMonitoringTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBMonitoringTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    def test_monitoring_response(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        mon_resp = self.monitoring_client.get_all_metrics(table_name)
        self.assertEqual(mon_resp[1]['size'], 0)
        self.assertEqual(mon_resp[1]['item_count'], 0)

    def test_project_monitoring_response(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        mon_resp = self.monitoring_client.get_all_project_metrics()

        table = None
        for t in mon_resp[1]:
            if t['table_name'] == table_name:
                table = t

        self.assertIsNotNone(table)
        self.assertEqual(table['usage_detailes']['item_count'], 0)
        self.assertEqual(table['usage_detailes']['size'], 0)

    def test_project_tables_monitoring_response(self):
        table_name = rand_name(self.table_prefix).replace('-', '')
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        mon_resp = self.monitoring_client.get_all_project_tables_metrics()

        table = None
        for t in mon_resp[1]:
            if (t['name'] == table_name and
                    t['tenant'] == self.monitoring_client.tenant_id):
                table = t

        self.assertIsNotNone(table)
        self.assertEqual(table['status'], 'ACTIVE')
        self.assertEqual(table['usage_detailes']['item_count'], 0)
        self.assertEqual(table['usage_detailes']['size'], 0)
