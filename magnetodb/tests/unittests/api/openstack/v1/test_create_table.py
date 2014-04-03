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
from magnetodb.storage import models

import mock
from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class CreateTableTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API CreateTableController."""

    @mock.patch('magnetodb.storage.create_table')
    def test_create_table(self, mock_create_table):
        mock_create_table.return_value = models.TableMeta(
            models.TableSchema(
                attribute_type_map={
                    "ForumName": models.ATTRIBUTE_TYPE_STRING,
                    "Subject": models.ATTRIBUTE_TYPE_STRING,
                    "LastPostDateTime": models.ATTRIBUTE_TYPE_STRING
                },
                key_attributes=["ForumName", "Subject"],
                index_def_map={
                    "LastPostIndex": models.IndexDefinition("LastPostDateTime")
                }
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables'
        body = """
            {
                "attribute_definitions": [
                    {
                        "attribute_name": "ForumName",
                        "attribute_type": "S"
                    },
                    {
                        "attribute_name": "Subject",
                        "attribute_type": "S"
                    },
                    {
                        "attribute_name": "LastPostDateTime",
                        "attribute_type": "S"
                    }
                ],
                "table_name": "Thread",
                "key_schema": [
                    {
                        "attribute_name": "ForumName",
                        "key_type": "HASH"
                    },
                    {
                        "attribute_name": "Subject",
                        "key_type": "RANGE"
                    }
                ],
                "local_secondary_indexes": [
                    {
                        "index_name": "LastPostIndex",
                        "key_schema": [
                            {
                                "attribute_name": "ForumName",
                                "key_type": "HASH"
                            },
                            {
                                "attribute_name": "LastPostDateTime",
                                "key_type": "RANGE"
                            }
                        ],
                        "projection": {
                            "projection_type": "KEYS_ONLY"
                        }
                    }
                ]
            }
        """

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
            'table_status': 'ACTIVE',
            'links': [
                {'href': table_url, 'rel': 'self'},
                {'href': table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_create_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)

    @mock.patch('magnetodb.storage.create_table')
    def test_create_table_no_sec_indexes(self, mock_create_table):
        mock_create_table.return_value = models.TableMeta(
            models.TableSchema(
                attribute_type_map={
                    "ForumName": models.ATTRIBUTE_TYPE_STRING,
                    "Subject": models.ATTRIBUTE_TYPE_STRING,
                    "LastPostDateTime": models.ATTRIBUTE_TYPE_STRING
                },
                key_attributes=["ForumName", "Subject"]
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE
        )

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables'
        body = """
            {
                "attribute_definitions": [
                    {
                        "attribute_name": "ForumName",
                        "attribute_type": "S"
                    },
                    {
                        "attribute_name": "Subject",
                        "attribute_type": "S"
                    },
                    {
                        "attribute_name": "LastPostDateTime",
                        "attribute_type": "S"
                    }
                ],
                "table_name": "Thread",
                "key_schema": [
                    {
                        "attribute_name": "ForumName",
                        "key_type": "HASH"
                    },
                    {
                        "attribute_name": "Subject",
                        "key_type": "RANGE"
                    }
                ]
            }
        """

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
            'table_name': 'Thread',
            'table_size_bytes': 0,
            'table_status': 'ACTIVE',
            'links': [
                {'href': table_url, 'rel': 'self'},
                {'href': table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_create_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)

    def test_create_table_malformed(self):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables'
        body = '{"table_name": "spam"}'

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "'attribute_definitions' is a required property",
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])
