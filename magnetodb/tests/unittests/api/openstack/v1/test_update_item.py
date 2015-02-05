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


class UpdateItemTestCase(test_base_testcase.APITestCase):
    """The test for update_method of openstack v1 ReST API."""

    @mock.patch('magnetodb.storage.update_item')
    def test_update_item(self, mock_update_item):

        mock_update_item.return_value = (True, None)

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/update_item'

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
                "attribute_updates": {
                    "LastPostedBy": {
                        "value": {
                            "S": "me@test.com"
                        },
                        "action": "PUT"
                    }
                },
                "expected": {
                    "LastPostedBy": {
                        "value": { "S": "fred@example.com"}
                    }
                }
            }
        """

        conn.request("POST", url, headers=headers, body=body)
        response = conn.getresponse()
        self.assertTrue(mock_update_item.called)
        self.assertEqual(200, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected = {}
        self.assertEqual(expected, response_payload)

    @mock.patch('magnetodb.storage.update_item')
    def test_update_item_del(self, mock_update_item):
        mock_update_item.return_value = (True, None)

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/update_item'

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
                "attribute_updates": {
                    "LastPostedBy": {
                        "action": "DELETE"
                    }
                }
            }
        """

        conn.request("POST", url, headers=headers, body=body)
        response = conn.getresponse()

        self.assertTrue(mock_update_item.called)

        kwargs = mock_update_item.call_args[1]
        attr = kwargs['attribute_action_map']['LastPostedBy']

        self.assertEqual('DELETE', attr['action'])
        self.assertIsNone(attr['value'])

        self.assertEqual(200, response.status)
        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual({}, response_payload)
