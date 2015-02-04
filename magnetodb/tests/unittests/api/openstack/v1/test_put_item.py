# Copyright 2014 Mirantis Inc.
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
import unittest

from oslo_serialization import jsonutils as json

from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class PutitemTestCase(test_base_testcase.APITestCase):
    """The test for v1 ReST API."""

    @mock.patch('magnetodb.storage.put_item')
    def test_put_item(self, mock_put_item):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}
        mock_put_item.return_value = (True, None)

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/put_item'
        body = """
            {
                "item": {
                    "LastPostDateTime": {
                        "S": "201303190422"
                    },
                    "Tags": {
                        "SS": ["Update","Multiple items","HelpMe"]
                    },
                    "ForumName": {
                        "S": "MagnetoDB"
                    },
                    "Message": {
                        "S": "I want to update multiple items."
                    },
                    "Subject": {
                        "S": "How do I update multiple items?"
                    },
                    "LastPostedBy": {
                        "S": "fred@example.com"
                    }
                },
                "expected": {
                    "ForumName": {
                        "exists": false
                    },
                    "Subject": {
                        "exists": false
                    }
                }
            }
        """
        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_put_item.called)
        expected_condition_map = mock_put_item.call_args[1][
            'expected_condition_map']
        self.assertTrue(isinstance(expected_condition_map['ForumName'], list))
        self.assertTrue(isinstance(expected_condition_map['Subject'], list))

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual({}, response_payload)

    @unittest.skip("bug: #1299037")
    @mock.patch('magnetodb.storage.put_item')
    def test_put_item_expected(self, mock_put_item):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/put_item'
        body = """
            {
                "item": {
                    "LastPostDateTime": {
                        "S": "201303190422"
                    },
                    "Tags": {
                        "SS": ["Update","Multiple items","HelpMe"]
                    },
                    "ForumName": {
                        "S": "MagnetoDB"
                    },
                    "Message": {
                        "S": "I want to update multiple items."
                    },
                    "Subject": {
                        "S": "How do I update multiple items?"
                    },
                    "LastPostedBy": {
                        "S": "fred@example.com"
                    }
                },
                "expected": {
                    "ForumName": {
                        "exists": false
                    },
                    "Subject": {
                        "exists": false
                    }
                },
                "return_values": "ALL_OLD"
            }
        """
        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_put_item.called)
        expected_condition_map = mock_put_item.call_args[1][
            'expected_condition_map']
        self.assertTrue(isinstance(expected_condition_map['ForumName'], list))
        self.assertTrue(isinstance(expected_condition_map['Subject'], list))

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected = {'attributes': {
            'ForumName': {'S': u'MagnetoDB'},
            'LastPostDateTime': {'S': '201303190422'},
            'LastPostedBy': {'S': 'fred@example.com'},
            'Message': {'S': 'I want to update multiple items.'},
            'Subject': {'S': 'How do I update multiple items?'},
            'Tags': {'SS': ['HelpMe', 'Multiple items', 'Update']}}}

        self.assertEqual(expected, response_payload)
