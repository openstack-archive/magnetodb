from funkload.FunkLoadDocTest import FunkLoadTestCase
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.regioninfo import RegionInfo
import unittest

#from tempest.thirdparty.boto


class LoadTest1(FunkLoadTestCase):

    def _create_email_security_table(self, conn, table_name):
        key_schema = [
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "date_message_id", "KeyType": "RANGE"}
        ]

        attribute_definitions = [
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "date_message_id", "AttributeType": "S"},
            {"AttributeName": "message_id", "AttributeType": "S"},
            {"AttributeName": "from_header", "AttributeType": "S"},
            {"AttributeName": "to_header", "AttributeType": "S"},
        ]

        local_secondary_indexes = [
            {
                "IndexName": "message_id_index",
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                    {"AttributeName": "message_id", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "from_header_index",
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                    {"AttributeName": "from_header", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
            {
                "IndexName": "to_header_index",
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                    {"AttributeName": "to_header", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            },
        ]

        provisioned_throughput = {"ReadCapacityUnits": 1,
                                  "WriteCapacityUnits": 1}

        table = conn.create_table(attribute_definitions, table_name,
                                  key_schema, provisioned_throughput,
                                  local_secondary_indexes,
                                  global_secondary_indexes=None)
        return table

    def test1(self):

        self.connection_data = {
            "aws_access_key_id": "AKIAJIM4GV44TAFS4VRA",
            "aws_secret_access_key": "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",
            "region": "magnetodb-1",
        }

        region_info = RegionInfo(name='magnetodb-1',
                                 endpoint='172.18.169.204',
                                 connection_cls=DynamoDBConnection)

        conn = region_info.connect(
            aws_access_key_id='AKIAJIM4GV44TAFS4VRA',
            aws_secret_access_key='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',
            is_secure=False, port=8080)

        table = self._create_email_security_table(conn, "yyekovenko_table1")

        #conn = DynamoDBConnection(**self.connection_data)

        resp = conn.list_tables()
        print resp

        #resp = conn.get_item(table_name='table--tempest-2034358728',
        #                     key={"attribute1": {"S": "item1"}})


        #assert "Item" in resp


if __name__ in ('main', '__main__'):
    unittest.main()
