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


class GetItemTestCase(test_base_testcase.APITestCase):
    """The test for get_item method of openstack v1 ReST API."""

    @mock.patch('magnetodb.storage.get_item')
    def test_get_item(self, mock_get_item):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/the_table/get_item'

        body = """
            {
                "key": {
                    "ForumName": {
                        "S": "MagnetoDB"
                    },
                    "Subject": {
                        "S": "How do I update multiple items?"
                    }
                },
                "attributes_to_get": ["LastPostDateTime","Message","Tags"],
                "consistent_read": true
            }
        """

        conn.request("POST", url, headers=headers, body=body)
        response = conn.getresponse()

        self.assertTrue(mock_get_item.called)

        json_response = response.read()

        response_payload = json.loads(json_response)
        self.assertEqual({'item': {}}, response_payload)
