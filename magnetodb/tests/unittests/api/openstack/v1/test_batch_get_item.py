# Copyright 2014 Symantec Corporation
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


class BatchGetItemTestCase(test_base_testcase.APITestCase):
    """The test for batch_get_item method for v1 ReST API."""

    @mock.patch('magnetodb.storage.execute_get_batch',
                return_value=([], []))
    def test_batch_get_item(self, mock_execute_select_batch):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/batch_get_item'
        body = """
            {
                "request_items": {
                    "Forum": {
                        "keys": [
                            {
                               "Name": {"S": "MagnetoDB"},
                               "Category": {"S": "OpenStack KVaaS"}
                            },
                            {
                               "Name": {"S": "Nova"},
                               "Category": {"S": "OpenStack Core"}
                            }
                        ]
                    },
                    "Thread": {
                        "keys": [
                            {
                               "Name": {"S": "MagnetoDB"},
                               "Category": {"S": "OpenStack KVaaS"}
                            },
                            {
                               "Name": {"S": "Nova"},
                               "Category": {"S": "OpenStack Core"}
                            }
                        ]
                    }
                }
            }
        """
        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertTrue(mock_execute_select_batch.called)
        expected = {'responses': {}, 'unprocessed_keys': {}}
        self.assertEqual(expected, response_payload)

    def test_batch_get_item_malformed(self):
        self.maxDiff = None
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/batch_get_item'
        body = '{"foo": "bar"}'
        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(400, response.status)

        expected_message = (
            "Required property 'request_items' wasn't found or "
            "it's value is null"
        )
        expected_type = 'ValidationError'

        self.assertEqual(expected_message,
                         response_payload['error']['message'])
        self.assertEqual(expected_type,
                         response_payload['error']['type'])
