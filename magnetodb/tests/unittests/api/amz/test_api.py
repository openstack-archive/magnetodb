# Copyright 2013 Mirantis Inc.
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
import unittest

from magnetodb import storage
from magnetodb.common.exception import TableNotExistsException
from magnetodb.tests.fake import magnetodb_api_fake
from mox import Mox, IgnoreArg


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

    def setUp(self):
        self.storage_mocker = Mox()

    def tearDown(self):
        self.storage_mocker.UnsetStubs()

    def test_describe_unexisting_table(self):
        self.storage_mocker.StubOutWithMock(storage, 'describe_table')

        storage.describe_table(IgnoreArg(), 'test_table1').AndRaise(
            TableNotExistsException
        )

        self.storage_mocker.ReplayAll()

        headers = {'Host': 'localhost:8080',
                   'Content-Type': 'application/x-amz-json-1.0',
                   'X-Amz-Target': 'DynamoDB_20120810.DescribeTable'}

        conn = httplib.HTTPConnection('localhost:8080')
        conn.request("POST", "/", body='{"TableName": "test_table1"}',
                     headers=headers)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)

        self.assertEqual(
            response_model['__type'],
            'com.amazonaws.dynamodb.v20111205#ResourceNotFoundException')

        self.assertEqual(
            response_model['message'],
            'The resource which is being requested does not exist.')

        self.assertEqual(400, response.status)

        self.assertEqual(
            response.getheader('Content-Type'), 'application/x-amz-json-1.0')
