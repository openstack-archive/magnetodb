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


class DeleteItemTestCase(test_base_testcase.APITestCase):
    """The test for delete_item method of openstack v1 ReST API."""

    @mock.patch('magnetodb.storage.delete_item')
    def test_delete_item(self, mock_delete_item):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/delete_item'

        # basic test case: delete item by key
        body = """
            {
                "key": {
                    "ForumName": {
                        "S": "MagnetoDB"
                    },
                    "Subject": {
                        "S": "How do I delete an item?"
                    }
                }
            }
        """

        conn.request("POST", url, headers=headers, body=body)
        response = conn.getresponse()
        self.assertTrue(mock_delete_item.called)
        self.assertEqual(200, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)
        self.assertEqual({}, response_payload)

    @mock.patch('magnetodb.storage.delete_item')
    def test_delete_item_with_expected_conditions(self, mock_delete_item):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/delete_item'

        # conditional delete item: in addition to key,
        # expected conditions and/or return values can be specified
        # expected conditions represents an attribute name to check,
        # along with value comparison
        # or value evaluation before attempting the conditional delete

        body = """
            {
                "key": {
                    "ForumName": {
                        "S": "MagnetoDB"
                    },
                    "Subject": {
                        "S": "How do I delete an item?"
                    }
                },
                "expected": {
                    "Subject": {
                        "value": {
                            "S": "How do I delete an item?"
                        }
                    },
                    "Replies": {
                        "exists": false
                    }
                },
                "return_values": "NONE"
            }
        """

        conn.request("POST", url, headers=headers, body=body)
        response = conn.getresponse()
        self.assertTrue(mock_delete_item.called)
        self.assertEqual(200, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)
        self.assertEqual({}, response_payload)

    def test_delete_item_malformed(self):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/delete_item'
        body = '{"foo": "bar"}'
        conn.request("POST", url, headers=headers, body=body)
        response = conn.getresponse()

        json_response = response.read()
        self.assertEqual(400, response.status)
        response_payload = json.loads(json_response)

        expected_message = (
            "Required property 'key' wasn't found or it's value is null"
        )
        expected_type = 'ValidationError'

        self.assertEqual(expected_message,
                         response_payload['error']['message'])
        self.assertEqual(expected_type,
                         response_payload['error']['type'])
