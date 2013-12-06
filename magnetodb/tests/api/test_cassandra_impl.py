# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import json
import os
import unittest

from magnetodb.common import PROJECT_ROOT_DIR
from magnetodb.common import config
from magnetodb.storage.impl import cassandra_impl as impl
from magnetodb.storage.models import AttributeDefinition
from magnetodb.storage.models import AttributeType
from magnetodb.storage.models import TableSchema

CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR,
                           'etc/magnetodb-test.conf')


class FakeContext(object):
    def __init__(self, tenant):
        self.tenant = tenant


class TestCassandraImpl(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCassandraImpl, cls).setUpClass()
        cls.orig_session = impl.SESSION
        cls.orig_cluster = impl.CLUSTER
        cls.orig_conf = config.CONF
        config.parse_args([], default_config_files=[CONFIG_FILE])
        storage_param = json.loads(config.CONF.storage_param)
        impl.CLUSTER = impl.cluster.Cluster(**storage_param)
        impl.SESSION = impl.CLUSTER.connect()

    @classmethod
    def tearDownClass(cls):
        super(TestCassandraImpl, cls).tearDownClass()
        impl.SESSION = cls.orig_session
        impl.CLUSTER = cls.orig_cluster
        config.CONF = cls.orig_conf

    def setUp(self):
        self.context = FakeContext('default_tenant')

    def test_crud_table(self):
        attrs = [
            AttributeDefinition('id',
                                AttributeType.ELEMENT_TYPE_NUMBER),
            AttributeDefinition('range',
                                AttributeType.ELEMENT_TYPE_STRING),
            AttributeDefinition('indexed',
                                AttributeType.ELEMENT_TYPE_STRING),
        ]

        schema = TableSchema('test', attrs, ['id', 'range'], ['indexed'])

        impl.create_table(self.context, schema)

        listed = impl.list_tables(self.context)
        self.assertEqual(['test'], listed)

        desc = impl.describe_table(self.context, 'test')

        self.assertEqual(schema, desc)

        impl.delete_table(self.context, 'test')

        listed = impl.list_tables(self.context)
        self.assertEqual([], listed)
