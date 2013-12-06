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

import boto
import os
import unittest

from boto.regioninfo import RegionInfo

from magnetodb.tests.fake import magnetodb_api_fake
from magnetodb.common import PROJECT_ROOT_DIR

CONF = magnetodb_api_fake.CONF


class Test(unittest.TestCase):
    PASTE_CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR,
                                     'etc/api-paste-test.ini')

    @classmethod
    def setUpClass(cls):
        super(Test, cls).setUpClass()

        magnetodb_api_fake.run_fake_magnetodb_api(cls.PASTE_CONFIG_FILE)
        cls.DYNAMODB_CON = cls.connect_boto_dynamodb()

    @classmethod
    def tearDownClass(cls):
        super(Test, cls).tearDownClass()
        magnetodb_api_fake.stop_fake_magnetodb_api()

    @staticmethod
    def connect_boto_dynamodb(host=None, port=None):
        if not host:
            host = CONF.bind_host

        if not port:
            port = CONF.bind_port

        endpoint = '{}:{}'.format(host, port)
        region = RegionInfo(name='test_server', endpoint=endpoint)
        return boto.connect_dynamodb(aws_access_key_id="asd",
                                     aws_secret_access_key="asd",
                                     region=region, port=port, is_secure=False,
                                     validate_certs=False)

    def testListTable(self):
        self.DYNAMODB_CON.list_tables()
