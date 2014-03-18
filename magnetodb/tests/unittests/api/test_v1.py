import httplib
import mock
import json
import unittest

from magnetodb.tests.fake import magnetodb_api_fake


class APITestCase(unittest.TestCase):
    """The test for v1 ReST API."""

    @classmethod
    def setUpClass(cls):
        magnetodb_api_fake.run_fake_magnetodb_api()

    @classmethod
    def tearDownClass(cls):
        magnetodb_api_fake.stop_fake_magnetodb_api()

    def test_list_tables(self):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables'
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)
        self.assertEqual([], response_model['Tables'])

    @mock.patch('magnetodb.storage.create_table')
    def test_create_table(self, mock_create_table):
        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/fake_project_id/data/tables'
        body = """
            {
                "AttributeDefinitions": [
                    {
                        "AttributeName": "ForumName",
                        "AttributeType": "S"
                    },
                    {
                        "AttributeName": "Subject",
                        "AttributeType": "S"
                    },
                    {
                        "AttributeName": "LastPostDateTime",
                        "AttributeType": "S"
                    }
                ],
                "TableName": "Thread",
                "KeySchema": [
                    {
                        "AttributeName": "ForumName",
                        "KeyType": "HASH"
                    },
                    {
                        "AttributeName": "Subject",
                        "KeyType": "RANGE"
                    }
                ],
                "LocalSecondaryIndexes": [
                    {
                        "IndexName": "LastPostIndex",
                        "KeySchema": [
                            {
                                "AttributeName": "ForumName",
                                "KeyType": "HASH"
                            },
                            {
                                "AttributeName": "LastPostDateTime",
                                "KeyType": "RANGE"
                            }
                        ],
                        "Projection": {
                            "ProjectionType": "KEYS_ONLY"
                        }
                    }
                ]
            }
        """

        table_url = ('http://localhost:8080/v1/fake_project_id'
                     '/data/tables/Thread')
        expected_response = {'TableDescription': {
            'AttributeDefinitions': [
                {'AttributeName': 'Subject', 'AttributeType': 'S'},
                {'AttributeName': 'LastPostDateTime', 'AttributeType': 'S'},
                {'AttributeName': 'ForumName', 'AttributeType': 'S'}
            ],
            'CreationDateTime': 0,
            'ItemCount': 0,
            'KeySchema': [
                {'AttributeName': 'ForumName', 'KeyType': 'HASH'},
                {'AttributeName': 'Subject', 'KeyType': 'RANGE'}
            ],
            'LocalSecondaryIndexes': [
                {'IndexName': 'LastPostIndex',
                 'IndexSizeBytes': 0,
                 'ItemCount': 0,
                 'KeySchema': [
                     {'AttributeName': 'ForumName',
                      'KeyType': 'HASH'},
                     {'AttributeName': 'LastPostDateTime',
                      'KeyType': 'RANGE'}
                 ],
                 'Projection': {'ProjectionType': 'ALL'}}
            ],
            'TableName': 'Thread',
            'TableSizeBytes': 0,
            'TableStatus': 'ACTIVE',
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
