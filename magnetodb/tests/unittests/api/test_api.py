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
import unittest

from magnetodb.tests.fake import magnetodb_api_fake
import mock


class APITest(unittest.TestCase):
    """
    The test for low level API calls
    """

    @classmethod
    def setUpClass(cls):
        magnetodb_api_fake.run_fake_magnetodb_api()

    @classmethod
    def tearDownClass(cls):
        magnetodb_api_fake.stop_fake_magnetodb_api()

    @mock.patch(
        "magnetodb.common.middleware.fault.FaultWrapper.process_request"
    )
    def test_connection_header(self, magnetodb_app_mock):
        magnetodb_app_mock.return_value = "{}"

        headers = {}

        conn = httplib.HTTPConnection('localhost:8080')
        conn.request("POST", "/", body='{}', headers=headers)

        response = conn.getresponse()

        self.assertEqual(response.getheader('Connection'), None)

        headers = {"Connection": "close"}

        conn = httplib.HTTPConnection('localhost:8080')
        conn.request("POST", "/", body='{}', headers=headers)

        response = conn.getresponse()

        self.assertEqual(response.getheader('Connection'), 'close')

        self.assertEqual(magnetodb_app_mock.call_count, 2)
