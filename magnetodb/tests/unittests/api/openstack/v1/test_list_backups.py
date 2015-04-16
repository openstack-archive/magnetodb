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

from oslo_serialization import jsonutils as json

from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class ListBackupsTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API ListBackupController."""
    @mock.patch('magnetodb.storage.list_backups')
    def test_list_backups(self, list_backups_mock):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/management/default_tenant/tables/default_table/backups'
        list_backups_mock.return_value = []
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)
        self.assertEqual([], response_model['backups'])
