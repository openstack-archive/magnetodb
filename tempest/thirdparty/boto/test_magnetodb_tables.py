# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 OpenStack Foundation
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

from random import choice
#import datetime

from tempest.common.utils import data_utils
from tempest.test import call_until_true
from tempest.thirdparty.boto.test import MagnetoDBTestCase


class MagnetoDBTablesTest(MagnetoDBTestCase):

    table_name = "yyekovenko_table1" # magnetodb
    #table_name = "EmailSecurity--tempest-489078663" # dynamodb

    def _create_email_security_table(self, table_name):
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

        table = self.client.create_table(attribute_definitions, table_name,
                                         key_schema, provisioned_throughput,
                                         local_secondary_indexes,
                                         global_secondary_indexes=None)
        return table

    def _create_email_security_table2(self, table_name):
        from boto.dynamodb2.table import Table, AllIndex, HashKey, RangeKey
        from boto.dynamodb2.fields import BaseSchemaField

        schema = [HashKey('user_id'),
                  RangeKey('date_message_id'),
                  BaseSchemaField('message_id'),
                  BaseSchemaField('from_header'),
                  BaseSchemaField('to_header')]

        indexes = [
            AllIndex('message_id_index',
                     parts=[HashKey('user_id'), RangeKey('message_id')]),
            AllIndex('from_header_index',
                     parts=[HashKey('user_id'), RangeKey('from_header')]),
            AllIndex('to_header_index',
                     parts=[HashKey('user_id'), RangeKey('to_header')])
        ]
        throughput = {"read": 1, "write": 1}

        return Table(table_name, schema, throughput, indexes, self.client)

    def _populate_email_security_table(self, table_name, usercount, itemcount):
        dates = ['2013-12-0%sT16:00:00.000001' % i for i in range(1, 8)]

        from_headers = ["%s@mail.com" % data_utils.rand_name()
                        for _ in range(10)]

        to_headers = ["%s@mail.com" % data_utils.rand_name()
                      for _ in range(10)]
        emails = []
        # TODO bug workaround
        expected = {}

        new_items = []
        for _ in range(usercount):
            email = "%s@mail.com" % data_utils.rand_name()
            emails.append(email)

            for item in range(itemcount):
                message_id = data_utils.rand_uuid()
                date = choice(dates)
                # put item
                item = {
                    "user_id": {"S": email},
                    "date_message_id": {"S": date + "#" + message_id},
                    "message_id": {"S": message_id},
                    "from_header": {"S": choice(from_headers)},
                    "to_header": {"S": choice(to_headers)},
                }
                new_items.append(item)
                resp = self.client.put_item(table_name, item, expected=expected)
        return new_items

    def _populate_email_security_table2(self, table_name, usercount, itemcount):
        from boto.dynamodb2.table import Table

        dates = ['2013-12-0%sT16:00:00.000001' % i for i in range(1, 8)]
        from_headers = ["%s@mail.com" % data_utils.rand_name()
                        for _ in range(10)]
        to_headers = ["%s@mail.com" % data_utils.rand_name()
                      for _ in range(10)]
        emails = []
        table = Table(table_name, self.client)
        for _ in range(usercount):
            email = '%s@mail.com' % data_utils.rand_name()
            emails.append(email)

            for _ in range(itemcount):
                message_id = data_utils.rand_uuid()
                resp = table.put_item({
                    'user_id': email,
                    'date_message_id': choice(dates) + "#" + message_id,
                    'message_id': message_id,
                    'from_header': choice(from_headers),
                    'to_header': choice(to_headers)}
                )

    def _wait_for_table_created(self, table_name, timeout=120, interval=3):
        def check():
            resp = self.client.describe_table(table_name)
            if "Table" in resp and "TableStatus" in resp["Table"]:
                return resp["Table"]["TableStatus"] == "ACTIVE"

        return call_until_true(check, timeout, interval)

    def create_table(self):
        table_name = data_utils.rand_name("EmailSecurity-")
        table = self._create_email_security_table(table_name)

        print table_name
        self.assertEqual(type(table), dict)
        self.assertEqual(table.TableName, table_name)
        self.assertEqual(table.TableStatus, "CREATING")
        self.assertTrue(self._wait_for_table_created(table_name))
        return table_name

    def put_item(self, table_name):
        return self._populate_email_security_table(self.table_name, 1, 3)
        pass

    def describe_table(self, table_name):
        resp = self.client.describe_table(table_name)
        print resp
        return resp

    def get_item(self, user_id, date_message_id):
        # todo these data taken from magnetodb. Don't delete (while no scan)
        #key = {"user_id": {"S": "test-tempest-555188452@mail.com"},
        #       "date_message_id": {
        #           "S": "2013-12-01T16:00:00.000001#"
        #                "4807956f-9c0b-407e-8cdb-e6357015e5d0"}}

        key = {"user_id": {"S": user_id},
               "date_message_id": {
                   "S": date_message_id}}

        resp = self.client.get_item(self.table_name, key=key)
        return resp

    def list_tables(self):
        resp = self.client.list_tables()
        return resp

    def query(self, user_id):

        key_conditions = {
            "user_id": {
                "AttributeValueList": [
                    #{"S": "test-tempest-1967233908@mail.com"}
                    {"S": user_id}
                ],
                "ComparisonOperator": "EQ"},
            "date_message_id": {
                "AttributeValueList": [
                    {"S": "2013-12-01"},
                    {"S": "2013-12-03"}
                ],
                "ComparisonOperator": "BETWEEN"
            }
        }
        resp = self.client.query(table_name=self.table_name,
                                 key_conditions=key_conditions)
        self.assertTrue(resp.Count > 0)
        return resp

    def test_scenario(self):
        tname = self.create_table()
        resp = self.describe_table(tname)
        tables = self.list_tables()
        new_items = self.put_item(tname)
        res = self.get_item(new_items[0]['user_id']['S'],
                            new_items[0]['date_message_id']['S'])

        query = self.query(new_items[0]['user_id']['S'])

