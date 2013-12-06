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

import unittest

from magnetodb.storage import models
from magnetodb.storage.impl import cassandra_impl


class FakeContext(object):
    def __init__(self, tenant):
        self.tenant = tenant


class TestCassandraImpl(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestCassandraImpl, cls).setUpClass()

        cls.CASANDRA_STORAGE_IMPL = cassandra_impl.CassandraStorageImpl(
            contact_points=("localhost",))

    @classmethod
    def tearDownClass(cls):
        super(TestCassandraImpl, cls).tearDownClass()

    def setUp(self):
        self.context = FakeContext('default_tenant')

    def test_crud_table(self):
        attrs = {
            models.AttributeDefinition(
                'id', models.AttributeType.ELEMENT_TYPE_NUMBER),
            models.AttributeDefinition(
                'range', models.AttributeType.ELEMENT_TYPE_STRING),
            models.AttributeDefinition(
                'indexed', models.AttributeType.ELEMENT_TYPE_STRING)
        }

        schema = models.TableSchema('test', attrs, {'id', 'range'},
                                    {'indexed'})

        self.CASANDRA_STORAGE_IMPL.create_table(self.context, schema)

        listed = self.CASANDRA_STORAGE_IMPL.list_tables(self.context)
        self.assertEqual(['test'], listed)

        desc = self.CASANDRA_STORAGE_IMPL.describe_table(self.context, 'test')

        self.assertEqual(schema, desc)

        self.CASANDRA_STORAGE_IMPL.delete_table(self.context, 'test')

        listed = self.CASANDRA_STORAGE_IMPL.list_tables(self.context)
        self.assertEqual([], listed)
