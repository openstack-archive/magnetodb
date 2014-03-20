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
from magnetodb.storage import models
from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class ScanTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API ScanController."""

    @mock.patch('magnetodb.storage.scan')
    def test_scan(self, mock_scan):

        items = [
            {
                'ForumName': models.AttributeValue.str('Gerrit workflow'),
                'LastPostDateTime': models.AttributeValue.str('3/19/14'),
                'Posts': models.AttributeValue.str_set(['Hi', 'Hello'])
            },
            {
                'ForumName': models.AttributeValue.str('Testing OS API'),
                'LastPostDateTime': models.AttributeValue.str('3/18/14'),
                'Posts': models.AttributeValue.str_set(['Opening post'])
            },
        ]

        last_evaluated_key = {
            'ForumName': models.AttributeValue.str('Testing OS API'),
            'Subject': models.AttributeValue.str('Some subject'),
        }

        mock_scan.return_value = models.ScanResult(
            items=items,
            last_evaluated_key=last_evaluated_key,
            count=2, scanned_count=10)

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables/Threads/scan'
        body = """
            {
               "attributes_to_get": [
                   "ForumName", "LastPostDateTime", "Posts"
               ],
               "exclusive_start_key":
                   {
                       "ForumName" :
                           {
                               "S": "Another forum"
                           }
                   },
               "limit": 2,
               "scan_filter":
                   {
                       "LastPostDateTime" :
                           {
                               "attribute_value_list": [
                                   {
                                       "S": "3/10/14"
                                   }
                               ],
                               "comparison_operator": "GT"
                           }
                   },
               "segment": 0,
               "select": "SPECIFIC_ATTRIBUTES",
               "total_segments": 1
            }
        """

        expected_response = {
            "count": 2,
            "items": [
                {
                    'ForumName': {'S': 'Gerrit workflow'},
                    'LastPostDateTime': {'S': '3/19/14'},
                    'Posts': {'SS': ['Hi', 'Hello']}
                },
                {
                    'ForumName': {'S': 'Testing OS API'},
                    'LastPostDateTime': {'S': '3/18/14'},
                    'Posts': {'SS': ['Opening post']}
                },
            ],
            "last_evaluated_key": {
                'ForumName': {'S': 'Testing OS API'},
                'Subject': {'S': 'Some subject'},
            },
            "scanned_count": 10
        }

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_scan.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)
