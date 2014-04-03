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
import mock

from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class BatchWriteItemTestCase(test_base_testcase.APITestCase):
    """The test for batch_write_item method for v1 ReST API."""

    @mock.patch('magnetodb.storage.execute_write_batch', return_value=[])
    def test_batch_write_item(self, mock_execute_write_batch):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/batch_write_item'
        body = """
            {
                "request_items": {
                    "Forum": [
                        {
                            "put_request": {
                                "item": {
                                    "Name": {"S": "MagnetoDB"},
                                    "Category": {"S": "OpenStack KVaaS"}
                                }
                            }
                        },
                        {
                            "put_request": {
                                "item": {
                                    "Name": {"S": "Nova"},
                                    "Category": {"S": "OpenStack Core"}
                                }
                            }
                        },
                        {
                            "put_request": {
                                "item": {
                                    "Name": {"S": "KeyStone"},
                                    "Category": {"S": "OpenStack Core"}
                                }
                            }
                        },
                        {
                            "delete_request": {
                                "key": {
                                    "Name": {"S": "Cinder"},
                                    "Category": {"S": "OpenStack Core"}
                                }
                            }
                        }
                    ]
                }
            }
        """
        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertTrue(mock_execute_write_batch.called)
        self.assertEqual({'unprocessed_items': {}}, response_payload)

    def test_batch_write_item_malformed(self):
        self.maxDiff = None
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/batch_write_item'
        body = '{"foo": "bar"}'
        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(400, response.status)

        expected_message = "'request_items' is a required property"
        expected_type = 'ValidationError'

        self.assertEqual(expected_message,
                         response_payload['error']['message'])
        self.assertEqual(expected_type,
                         response_payload['error']['type'])
