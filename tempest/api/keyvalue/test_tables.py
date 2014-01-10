# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 OpenStack Foundation
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

from tempest.api.keyvalue.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name


class MagnetoDBTablesTest(MagnetoDBTestCase):

    # smoke
    def test_table_create_list_delete(self):
        tname = rand_name().replace('-', '')
        table = self.client.create_table(self.smoke_attrs,
                                         tname,
                                         self.smoke_schema,
                                         self.smoke_throughput,
                                         self.smoke_lsi,
                                         self.smoke_gsi)
        rck = self.addResourceCleanUp(self.client.delete_table, tname)
        self.assertEqual(type(table), dict)
        # TODO(yyekovenko) Later should be changed to just "CREATING" (async)
        self.assertIn(table['TableDescription']['TableStatus'],
                      ['CREATING', 'ACTIVE'])
        self.assertEqual(table['TableDescription']['TableName'], tname)
        self.assertTrue(self.wait_for_table_active(tname))

        tables = self.client.list_tables()
        self.assertIn(tname, tables['TableNames'])

        descr = self.client.describe_table(tname)
        self.assertIn('Table', descr)
        self.assertIn('ItemCount', descr['Table'])

        res = self.client.delete_table(tname)
        # TODO(yyekovenko) Later should be changed to just "DELETING" (async)
        self.assertIn(res['TableDescription']['TableStatus'],
                      ['DELETING', 'ACTIVE'])
        self.assertTrue(self.wait_for_table_deleted(tname))
        self.cancelResourceCleanUp(rck)
