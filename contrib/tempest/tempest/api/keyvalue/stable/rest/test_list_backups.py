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
from tempest_lib.common.utils.data_utils import rand_name


class MagnetoDBListBackupsTest(MagnetoDBTestCase):
    def setUp(self):
        super(MagnetoDBListBackupsTest, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    def test_list_backups(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)

        bname1 = rand_name(self.table_prefix).replace('-', '')
        self.management_client.create_backup(
            self.tname, bname1, {'name': 'default'})

        bname2 = rand_name(self.table_prefix).replace('-', '')
        self.management_client.create_backup(
            self.tname, bname2, {'name': 'default'})

        bname3 = rand_name(self.table_prefix).replace('-', '')
        self.management_client.create_backup(
            self.tname, bname3, {'name': 'default'})

        headers, body = self.management_client.list_backups(
            self.tname, limit=2)
        self.assertEqual(2, len(body['backups']))

        last_eval_id = body['last_evaluated_backup_id']
        backups1 = body['backups']

        headers, body = self.management_client.list_backups(
            self.tname, exclusive_start_backup_id=last_eval_id, limit=2)
        self.assertEqual(1, len(body['backups']))

        backups2 = body['backups']

        bnames = [b['backup_name'] for b in backups1]
        bnames.extend([b['backup_name'] for b in backups2])

        self.assertEqual(3, len(bnames))
        self.assertTrue(bname1 in bnames)
        self.assertTrue(bname2 in bnames)
        self.assertTrue(bname3 in bnames)
