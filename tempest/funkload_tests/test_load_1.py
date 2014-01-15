from funkload.FunkLoadDocTest import FunkLoadTestCase
from boto.dynamodb2.layer1 import DynamoDBConnection
from tempest.common.utils.data_utils import rand_name
from tempest.common.utils.data_utils import rand_uuid
import unittest
import random


class LoadTest1(FunkLoadTestCase):

    @classmethod
    def do_preconditions(cls):

        cls.attrs = [
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "date_message_id", "AttributeType": "S"},
            {"AttributeName": "message_id", "AttributeType": "S"},
            {"AttributeName": "from_header", "AttributeType": "S"},
            {"AttributeName": "to_header", "AttributeType": "S"},
        ]
        cls.gsi = None
        cls.lsi = [
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
        cls.schema = [
            {"AttributeName": "user_id", "KeyType": "HASH"},
            {"AttributeName": "date_message_id", "KeyType": "RANGE"}
        ]
        cls.throughput = {
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        }
        cls.table_name = rand_name().replace('-', '')

        cls.connection_data = {
            'aws_access_key_id': '',
            'aws_secret_access_key': '',
            'region': 'magnetodb-1',
            'host': '172.18.169.204',
            'port': '8080',
            'is_secure': False
        }
        cls.conn = DynamoDBConnection(**cls.connection_data)

        if cls.table_name not in cls.conn.list_tables()['TableNames']:
            cls.conn.create_table(cls.attrs,
                                  cls.table_name,
                                  cls.schema,
                                  cls.throughput,
                                  cls.lsi,
                                  cls.gsi)

        cls.user_id = 'user@mail.com'
        cls.date = '2013-12-31'
        cls.message_id = '123456'
        cls.date_message_id = '{}#{}'.format(cls.date, cls.message_id)
        cls.key = {'user_id': {'S': cls.user_id},
                   'date_message_id': {'S': cls.date_message_id}}

        item = cls.build_spam_item(cls.user_id, cls.date_message_id,
                                   cls.message_id, 'from@mail.com',
                                   'to@mail.com')
        cls.conn.put_item(cls.table_name, item)

    @classmethod
    def do_postconditions(cls):
        cls.conn.delete_table(cls.table_name)

    @classmethod
    def setUpClass(cls):
        """
        Method is called only if functional test is run.
        For benchmark setUpBench is run instead.
        """
        super(LoadTest1, cls).setUpClass()
        cls.do_preconditions()

    @classmethod
    def tearDownClass(cls):
        cls.do_postconditions()

    def setUpBench(self):
        self.do_preconditions()

    def tearDownBench(self):
        self.do_postconditions()

    @staticmethod
    def build_spam_item(user_id, date_message_id, message_id,
                        from_header, to_header):
        return {
            "user_id": {"S": user_id},
            "date_message_id": {"S": date_message_id},
            "message_id": {"S": message_id},
            "from_header": {"S": from_header},
            "to_header": {"S": to_header},
        }

    def populate_spam_table(self, table_name, usercount, itemcount):

        dates = ['2013-12-0%sT16:00:00.000001' % i for i in range(1, 8)]
        from_headers = ["%s@mail.com" % rand_name() for _ in range(10)]
        to_headers = ["%s@mail.com" % rand_name() for _ in range(10)]
        emails = []

        new_items = []
        for _ in range(usercount):
            email = "%s@mail.com" % rand_name()
            emails.append(email)

            for item in range(itemcount):
                message_id = rand_uuid()
                date = random.choice(dates)
                # put item
                item = {
                    "user_id": {"S": email},
                    "date_message_id": {"S": date + "#" + message_id},
                    "message_id": {"S": message_id},
                    "from_header": {"S": random.choice(from_headers)},
                    "to_header": {"S": random.choice(to_headers)},
                }
                new_items.append(item)
                self.conn.put_item(table_name, item)
        return new_items

    def test_load_describe_table(self):
        resp = self.conn.describe_table(self.table_name)
        self.assertTrue(resp['Table']['TableName'] == self.table_name)

    def test_load_list_tables(self):
        self.conn.list_tables()

    def test_load_get_item(self):
        resp = self.conn.get_item(self.table_name, self.key)
        assert "Item" in resp


if __name__ in ('main', '__main__'):
    unittest.main()
