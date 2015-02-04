# Copyright 2014 Symantec Corp.
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


class ListTablesTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API ListTableController."""
    @mock.patch('magnetodb.storage.list_tables')
    def test_list_tables(self, mock_list_tables):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables'
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)
        self.assertEqual([], response_model['tables'])

    def test_list_tables_invalid_url_parameters(self):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables?start_table_name=aaa'
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Unexpected properties were found for 'params': "
            "MultiDict([(u'start_table_name', u'aaa')])",
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])
