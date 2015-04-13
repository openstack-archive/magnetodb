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

import uuid

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name


class MagnetoDBListRestoresTest(MagnetoDBTestCase):
    def setUp(self):
        super(MagnetoDBListRestoresTest, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    def test_list_restore_jobs(self):
        self._create_test_table(self.smoke_attrs,
                                self.tname,
                                self.smoke_schema,
                                wait_for_active=True)

        buuid1 = uuid.uuid4()
        self.management_client.create_restore_job(
            self.tname, buuid1.hex)

        buuid2 = uuid.uuid4()
        self.management_client.create_restore_job(
            self.tname, buuid2.hex)

        buuid3 = uuid.uuid4()
        self.management_client.create_restore_job(
            self.tname, buuid3.hex)

        headers, body = self.management_client.list_restore_jobs(
            self.tname, limit=2)
        self.assertEqual(2, len(body['restore_jobs']))

        last_eval_id = body['last_evaluated_restore_job_id']
        restore_jobs1 = body['restore_jobs']

        headers, body = self.management_client.list_restore_jobs(
            self.tname, exclusive_start_restore_job_id=last_eval_id, limit=2)
        self.assertEqual(1, len(body['restore_jobs']))

        restore_jobs2 = body['restore_jobs']

        buuids = [b['backup_id'] for b in restore_jobs1]
        buuids.extend([b['backup_id'] for b in restore_jobs2])

        self.assertEqual(3, len(buuids))
        self.assertTrue(buuid1.hex in buuids)
        self.assertTrue(buuid2.hex in buuids)
        self.assertTrue(buuid3.hex in buuids)
