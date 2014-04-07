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


class QueryTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API ScanController."""

    @mock.patch('magnetodb.storage.select_item')
    def test_query(self, mock_query):

        items = [
            {
                'ForumName': models.AttributeValue.str('Testing OS API'),
                'LastPostDateTime': models.AttributeValue.str('3/18/14'),
                'Posts': models.AttributeValue.str_set(['Opening post'])
            },
            {
                'ForumName': models.AttributeValue.str('Testing OS API'),
                'LastPostDateTime': models.AttributeValue.str('3/19/14'),
                'Posts': models.AttributeValue.str_set(['Hi', 'Hello'])
            },
        ]

        last_evaluated_key = {
            'ForumName': models.AttributeValue.str('Testing OS API'),
            'LastPostDateTime': models.AttributeValue.str('3/19/14'),
        }

        mock_query.return_value = models.SelectResult(
            items=items,
            last_evaluated_key=last_evaluated_key,
            count=2)

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables/Threads/query'
        body = """
            {
               "attributes_to_get": [
                   "ForumName", "LastPostDateTime", "Posts"
               ],
               "exclusive_start_key":
                   {
                       "ForumName" :
                           {
                               "S": "Testing OS API"
                           },
                       "LastPostDayTime" :
                           {
                               "S": "3/1/14"
                           }
                   },
               "index_name": "LastPostIndex",
               "limit": 2,
               "consistent_read": true,
               "scan_index_forward": true,
               "key_conditions":
                   {
                        "ForumName" :
                           {
                               "attribute_value_list": [
                                   {
                                       "S": "Testing OS API"
                                   }
                               ],
                               "comparison_operator": "EQ"
                           },
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
               "select": "SPECIFIC_ATTRIBUTES"
            }
        """

        expected_response = {
            "count": 2,
            "items": [
                {
                    'ForumName': {'S': 'Testing OS API'},
                    'LastPostDateTime': {'S': '3/18/14'},
                    'Posts': {'SS': ['Opening post']}
                },
                {
                    'ForumName': {'S': 'Testing OS API'},
                    'LastPostDateTime': {'S': '3/19/14'},
                    'Posts': {'SS': ['Hi', 'Hello']}
                },
            ],
            "last_evaluated_key": {
                'ForumName': {'S': 'Testing OS API'},
                'LastPostDateTime': {'S': '3/19/14'},
            }
        }

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_query.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)

    @mock.patch('magnetodb.storage.select_item')
    def test_query_count(self, mock_query):
        mock_query.return_value = models.SelectResult(count=100500)

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables/Threads/query'
        body = """
            {
                "key_conditions":
                   {
                        "ForumName" :
                           {
                               "attribute_value_list": [
                                   {
                                       "S": "Testing OS API"
                                   }
                               ],
                               "comparison_operator": "EQ"
                           },
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
               "select": "COUNT"
            }
        """

        expected_response = {
            "count": 100500,
        }

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_query.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)
