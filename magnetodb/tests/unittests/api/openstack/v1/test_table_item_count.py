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
import json
from magnetodb.storage import models

from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase
import mock


class TableItemCountTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API TableItemCountController."""

    @mock.patch('magnetodb.storage.table_item_count')
    def test_table_item_count(self, mock_table_item_count):
        mock_table_item_count.return_value = models.TableItemCountRequest(count=100500)
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/monitoring/default_tenant/tables/the_table'
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        self.assertTrue(mock_table_item_count.called)

        json_response = response.read()
        response_model = json.loads(json_response)

        self.assertEqual(100500, response_model['item_count'])
