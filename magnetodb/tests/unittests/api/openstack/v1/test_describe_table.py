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

from magnetodb.storage import models
from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class DescribeTableTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API DescribeTableController."""

    @mock.patch('magnetodb.storage.describe_table')
    def test_describe_table(self, mock_describe_table):

        attr_map = {'ForumName': models.AttributeType('S'),
                    'Subject': models.AttributeType('S'),
                    'LastPostDateTime': models.AttributeType('S')}

        key_attrs = ['ForumName', 'Subject']

        index_map = {
            'LastPostIndex': models.IndexDefinition('ForumName',
                                                    'LastPostDateTime')
        }

        table_meta = models.TableMeta(
            '00000000-0000-0000-0000-000000000000',
            models.TableSchema(attr_map, key_attrs, index_map),
            models.TableMeta.TABLE_STATUS_ACTIVE,
            123)

        mock_describe_table.return_value = table_meta

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables/Thread'

        table_url = ('http://localhost:8080/v1/data/default_tenant'
                     '/tables/Thread')
        expected_response = {'table': {
            'attribute_definitions': [
                {'attribute_name': 'Subject', 'attribute_type': 'S'},
                {'attribute_name': 'LastPostDateTime', 'attribute_type': 'S'},
                {'attribute_name': 'ForumName', 'attribute_type': 'S'}
            ],
            'creation_date_time': 123,
            'item_count': 0,
            'key_schema': [
                {'attribute_name': 'ForumName', 'key_type': 'HASH'},
                {'attribute_name': 'Subject', 'key_type': 'RANGE'}
            ],
            'local_secondary_indexes': [
                {'index_name': 'LastPostIndex',
                 'index_size_bytes': 0,
                 'item_count': 0,
                 'key_schema': [
                     {'attribute_name': 'ForumName',
                      'key_type': 'HASH'},
                     {'attribute_name': 'LastPostDateTime',
                      'key_type': 'RANGE'}
                 ],
                 'projection': {'projection_type': 'ALL'}}
            ],
            'table_id': '00000000-0000-0000-0000-000000000000',
            'table_name': 'Thread',
            'table_size_bytes': 0,
            'table_status': 'ACTIVE',
            'links': [
                {'href': table_url, 'rel': 'self'},
                {'href': table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        self.assertTrue(mock_describe_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)
