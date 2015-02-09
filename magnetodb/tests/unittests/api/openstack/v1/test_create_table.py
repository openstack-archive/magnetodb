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


class CreateTableTest(test_base_testcase.APITestCase):
    """The test for v1 ReST API CreateTableController."""

    def setUp(self):
        super(CreateTableTest, self).setUp()
        self.headers = {'Content-Type': 'application/json',
                        'Accept': 'application/json'}

        self.url = '/v1/data/default_tenant/tables'

        self.table_url = ('http://localhost:8080/v1/data/default_tenant'
                          '/tables/Thread')

    @mock.patch('magnetodb.storage.create_table')
    def test_create_table(self, mock_create_table):
        mock_create_table.return_value = models.TableMeta(
            '00000000-0000-0000-0000-000000000000',
            models.TableSchema(
                attribute_type_map={
                    "ForumName": models.AttributeType('S'),
                    "Subject": models.AttributeType('S'),
                    "LastPostDateTime": models.AttributeType('S')
                },
                key_attributes=["ForumName", "Subject"],
                index_def_map={
                    "LastPostIndex": models.IndexDefinition("ForumName",
                                                            "LastPostDateTime")
                }
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE,
            123
        )

        conn = httplib.HTTPConnection('localhost:8080')
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

        expected_response = {'table_description': {
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
                {'href': self.table_url, 'rel': 'self'},
                {'href': self.table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_create_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)

    @mock.patch('magnetodb.storage.create_table')
    def test_create_table_no_sec_indexes(self, mock_create_table):
        mock_create_table.return_value = models.TableMeta(
            '00000000-0000-0000-0000-000000000000',
            models.TableSchema(
                attribute_type_map={
                    "ForumName": models.AttributeType('S'),
                    "Subject": models.AttributeType('S'),
                    "LastPostDateTime": models.AttributeType('S')
                },
                key_attributes=["ForumName", "Subject"]
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE,
            123
        )

        conn = httplib.HTTPConnection('localhost:8080')
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

        expected_response = {'table_description': {
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
            'table_id': '00000000-0000-0000-0000-000000000000',
            'table_name': 'Thread',
            'table_size_bytes': 0,
            'table_status': 'ACTIVE',
            'links': [
                {'href': self.table_url, 'rel': 'self'},
                {'href': self.table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertTrue(mock_create_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)

    def test_create_table_malformed(self):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables'
        body = '{"table_name": "spam"}'

        conn.request("POST", url, headers=headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Required property 'attribute_definitions' wasn't"
                       " found or it's value is null",
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    @mock.patch('magnetodb.storage.create_table')
    def test_create_table_no_range(self, mock_create_table):
        mock_create_table.return_value = models.TableMeta(
            '00000000-0000-0000-0000-000000000000',
            models.TableSchema(
                attribute_type_map={
                    "ForumName": models.AttributeType('S'),
                    "Subject": models.AttributeType('S'),
                    "LastPostDateTime": models.AttributeType('S')
                },
                key_attributes=["ForumName"]
            ),
            models.TableMeta.TABLE_STATUS_ACTIVE,
            123
        )
        conn = httplib.HTTPConnection('localhost:8080')
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
                    }
                ]
            }
        """

        expected_response = {'table_description': {
            'attribute_definitions': [
                {'attribute_name': 'Subject', 'attribute_type': 'S'},
                {'attribute_name': 'LastPostDateTime', 'attribute_type': 'S'},
                {'attribute_name': 'ForumName', 'attribute_type': 'S'}
            ],
            'creation_date_time': 123,
            'item_count': 0,
            'key_schema': [
                {'attribute_name': 'ForumName', 'key_type': 'HASH'}
            ],
            'table_id': '00000000-0000-0000-0000-000000000000',
            'table_name': 'Thread',
            'table_size_bytes': 0,
            'table_status': 'ACTIVE',
            'links': [
                {'href': self.table_url, 'rel': 'self'},
                {'href': self.table_url, 'rel': 'bookmark'}
            ]}}

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()
        self.assertEqual(200, response.status)

        self.assertTrue(mock_create_table.called)

        json_response = response.read()
        response_payload = json.loads(json_response)

        self.assertEqual(expected_response, response_payload)

    def test_create_table_index_invalid_key_type(self):
        conn = httplib.HTTPConnection('localhost:8080')

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

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Table without range key in primary key schema "
                       "can not have indices",
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_invalid_name_character(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_table_name = 'Thread Table'

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
                "table_name": "%s",
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
        """ % (invalid_table_name)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Wrong table name '%s' found" % (invalid_table_name),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_invalid_name_limit(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_table_name = 'Th'

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
                "table_name": "%s",
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
        """ % (invalid_table_name)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Wrong table name '%s' found" % (invalid_table_name),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_index_invalid_name_character(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_index_name = 'Last Post Index'

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
                        "index_name": "%s",
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
        """ % (invalid_index_name)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Wrong index name '%s' found" % (invalid_index_name),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_index_invalid_name_limit(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_index_name = 'LP'

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
                        "index_name": "%s",
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
        """ % (invalid_index_name)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Wrong index name '%s' found" % (invalid_index_name),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_attribute_invalid_name_limit(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_attr_name = 'A' * 256

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
                        "attribute_name": "%s",
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
        """ % (invalid_attr_name)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Wrong attribute name '%s' found" % (invalid_attr_name),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_no_type_validation_with_primary_hash_key(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_primary_hash_key_type = 'SS'

        body = """
            {
                "attribute_definitions": [
                    {
                        "attribute_name": "ForumName",
                        "attribute_type": "%s"
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
        """ % (invalid_primary_hash_key_type)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Type '%s' is not a scalar type"
                       % (invalid_primary_hash_key_type),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])

    def test_create_table_no_type_validation_with_primary_range_key(self):
        conn = httplib.HTTPConnection('localhost:8080')

        invalid_primary_range_key_type = 'SS'

        body = """
            {
                "attribute_definitions": [
                    {
                        "attribute_name": "ForumName",
                        "attribute_type": "S"
                    },
                    {
                        "attribute_name": "Subject",
                        "attribute_type": "%s"
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
        """ % (invalid_primary_range_key_type)

        conn.request("POST", self.url, headers=self.headers, body=body)

        response = conn.getresponse()

        self.assertEqual(400, response.status)

        json_response = response.read()
        response_payload = json.loads(json_response)

        expected_error = {
            'message': "Type '%s' is not a scalar type"
                       % (invalid_primary_range_key_type),
            'traceback': None,
            'type': 'ValidationError',
        }

        self.assertEqual(expected_error, response_payload['error'])
