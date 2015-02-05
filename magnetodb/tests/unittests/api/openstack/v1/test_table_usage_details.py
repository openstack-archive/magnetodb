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


class TableUsageTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API TableUsageController."""

    @mock.patch('magnetodb.storage.get_table_statistics')
    def test_table_usage_details(self, mock_get_table_statistics):
        mock_get_table_statistics.return_value = {
            'size': 500,
            'item_count': 100
        }
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/monitoring/projects/default_tenant/tables/the_table?' \
              'metrics=size,item_count'
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        self.assertTrue(mock_get_table_statistics.called)

        json_response = response.read()
        response_model = json.loads(json_response)

        self.assertEqual(100, response_model['item_count'])
        self.assertEqual(500, response_model['size'])
