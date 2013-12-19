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

    table_name = "yyekovenko_table1"
    #table_name = "EmailSecurity--tempest-489078663"

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

    def _populate_email_security_table(self, table_name, usercount, itemcount):
        # populate the table
        dates = ['2013-12-01T16:00:00.000001', '2013-12-02T17:00:00.000001',
                 '2013-12-03T18:00:00.000001', '2013-12-04T17:00:00.000001',
                 '2013-12-05T20:00:00.000001', '2013-12-06T21:00:00.000001',
                 '2013-12-07T22:00:00.000001']

        from_headers = ["%s@mail.com" % data_utils.rand_name()
                        for _ in range(10)]

        to_headers = ["%s@mail.com" % data_utils.rand_name()
                      for _ in range(10)]
        emails = []

        expected = {}

        for _ in range(usercount):
            # generate emails
            email = "%s@mail.com" % data_utils.rand_name()
            emails.append(email)

            for item in range(itemcount):
                message_id = data_utils.rand_uuid()
                #date = datetime.datetime.now().isoformat()
                date = choice(dates)
                # put item
                item = {
                    "user_id": {"S": email},
                    "date_message_id": {"S": date + "#" + message_id},
                    "message_id": {"S": message_id},
                    "from_header": {"S": choice(from_headers)},
                    "to_header": {"S": choice(to_headers)},
                }
                resp = self.client.put_item(table_name, item, expected=expected)

    def _wait_for_table_created(self, table_name, timeout=120, interval=3):
        def check():
            resp = self.client.describe_table(table_name)
            if "Table" in resp and "TableStatus" in resp["Table"]:
                return resp["Table"]["TableStatus"] == "ACTIVE"

        return call_until_true(check, timeout, interval)

    def test_create_table(self):

        table_name = data_utils.rand_name("EmailSecurity-")
        table = self._create_email_security_table(table_name)
        #
        print table_name
        self.assertEqual(type(table), dict)
        self.assertEqual(table.TableName, table_name)
        self.assertEqual(table.TableStatus, "CREATING")
        #
        self.assertTrue(self._wait_for_table_created(table_name))

    def test_put_item(self):
        #table_name = "EmailSecurity--tempest-489078663"
        self._populate_email_security_table(self.table_name, 1, 3)
        pass

    def test_describe_table(self):
        resp = self.client.describe_table(self.table_name)
        print resp

    def test_get_item(self):
        # todo these data taken from magnetodb. Don't delete (while no scan)
        key = {"user_id": {"S": "test-tempest-555188452@mail.com"},
               "date_message_id": {
                   "S": "2013-12-01T16:00:00.000001#"
                        "4807956f-9c0b-407e-8cdb-e6357015e5d0"}}

        resp = self.client.get_item(self.table_name, key=key)
        print resp
        pass

    def test_list_tables(self):
        resp = self.client.list_tables()
        print resp

    def test_query(self):

        key_conditions = {
            "user_id": {
                "AttributeValueList": [
                    {"S": "test-tempest-1967233908@mail.com"}
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
        for i in resp.Items:
            print i
