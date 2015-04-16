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


class CreateRestoreJobTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API CreateRestoreJobController."""
    @mock.patch('magnetodb.storage.create_restore_job')
    def test_create_restore_job(self, create_restore_job_mock):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/management/default_tenant/tables/default_table/restores'

        backup_uuid = uuid.uuid4()

        body = '{{ "backup_id": "{}", "source": "the_source"}}'.format(
            backup_uuid.hex)

        the_uuid = uuid.uuid4()

        create_restore_job_mock.return_value = models.RestoreJobMeta(
            the_uuid,
            'default_table',
            models.RestoreJobMeta.RESTORE_STATUS_RESTORING,
            backup_uuid,
            'the_source'
        )

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)

        self.assertEqual('default_table', response_model['table_name'])
        self.assertEqual(backup_uuid.hex, response_model['backup_id'])
        self.assertEqual('the_source', response_model['source'])
