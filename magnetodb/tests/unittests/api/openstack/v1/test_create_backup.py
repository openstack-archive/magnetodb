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

import httplib
import mock
import uuid

from oslo_serialization import jsonutils as json

from magnetodb.storage import models
from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class CreateBackupTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API CreateBackupController."""

    @mock.patch('magnetodb.storage.create_backup')
    def test_create_backup(self, create_backup_mock):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/management/default_tenant/tables/default_table/backups'
        body = """
            {
                "backup_name": "the_backup",
                "strategy": {}
            }
        """

        the_uuid = uuid.uuid4()

        create_backup_mock.return_value = models.BackupMeta(
            the_uuid,
            'the_backup',
            'default_table',
            models.BackupMeta.BACKUP_STATUS_CREATING,
            'location'
        )

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)
        self.assertEqual('default_table', response_model['table_name'])
        self.assertEqual('the_backup', response_model['backup_name'])
