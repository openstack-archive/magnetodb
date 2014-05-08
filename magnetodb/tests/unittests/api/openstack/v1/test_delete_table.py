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


class DeleteTableTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API DeleteTableController."""

    @mock.patch('magnetodb.storage.delete_table')
    @mock.patch('magnetodb.storage.describe_table')
    def test_delete_table(self, mock_describe_table, mock_delete_table):

        attr_map = {'ForumName': models.ATTRIBUTE_TYPE_STRING,
                    'Subject': models.ATTRIBUTE_TYPE_STRING,
                    'LastPostDateTime': models.ATTRIBUTE_TYPE_STRING}

        key_attrs = ['ForumName', 'Subject']

        index_map = {
            'LastPostIndex': models.IndexDefinition('LastPostDateTime')
        }

        table_meta = models.TableMeta(
            models.TableSchema(attr_map, key_attrs, index_map),
            models.TableMeta.TABLE_STATUS_DELETING)

        mock_delete_table.return_value = table_meta

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables/Thread'

        table_url = ('http://localhost:8080/v1/fake_project_id'
                     '/data/tables/Thread')
        expected_response = {'table_description': {
            'attribute_definitions': [
                {'attribute_name': 'Subject', 'attribute_type': 'S'},
                {'attribute_name': 'LastPostDateTime', 'attribute_type': 'S'},
                {'attribute_name': 'ForumName', 'attribute_type': 'S'}
            ],
            'creation_date_time': 0,
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
            'table_name': 'Thread',
            'table_size_bytes': 0,
            'table_status': 'DELETING',
            'links': [
                {'href': table_url, 'rel': 'self'},
                {'href': table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("DELETE", url, headers=headers)

        response = conn.getresponse()

        self.assertTrue(mock_delete_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)
