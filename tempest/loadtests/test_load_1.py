from funkload.FunkLoadDocTest import FunkLoadTestCase
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.regioninfo import RegionInfo
import unittest


class LoadTest1(FunkLoadTestCase):

    use_magneto = True
    if use_magneto:
        table_name = "yyekovenko_table1"
    else:
        table_name = 'EmailSecurity--tempest-489078663'

    def setUp(self):
        if self.use_magneto:
            self.connection_data = {
                "aws_access_key_id": "",
                "aws_secret_access_key": "",
                "region": RegionInfo(name='magnetodb-1',
                                     endpoint='172.18.169.204',
                                     connection_cls=DynamoDBConnection),
                "port": 8080,
                "is_secure": False
            }
        else:
            self.connection_data = {
                "aws_access_key_id": "AKIAJIM4GV44TAFS4VRA",
                "aws_secret_access_key":
                "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",
                "region": RegionInfo(
                    name='us-east-1',
                    endpoint='dynamodb.us-east-1.amazonaws.com',
                    connection_cls=DynamoDBConnection)
            }
        self.conn = DynamoDBConnection(**self.connection_data)

    def test_load_describe_table(self):
        resp = self.conn.describe_table(table_name=self.table_name)
        self.assertTrue(resp['Table']['TableName'] == self.table_name)

    def test_load_list_tables(self):
        resp = self.conn.list_tables()
        self.assertTrue()

    def test_load_get_item(self):
        if self.use_magneto:
            key = {"user_id": {"S": "test-tempest-555188452@mail.com"},
                   "date_message_id": {
                       "S": "2013-12-01T16:00:00.000001#"
                            "4807956f-9c0b-407e-8cdb-e6357015e5d0"}}
        else:
            key = {"user_id": {"S": "test-tempest-1967233908@mail.com"},
                   "date_message_id": {
                       "S": "2013-12-01T16:00:00.000001#"
                            "052fc4fd-6260-4e80-a6db-57b3af184fdc"}}

        resp = self.conn.get_item(table_name=self.table_name,
                                  key=key)
        assert "Item" in resp


if __name__ in ('main', '__main__'):
    unittest.main()
