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

import random

#from boto import exception

import tempest.clients
import tempest.config
import tempest.test
from tempest.openstack.common import log as logging
# Added by yyekovenko
from tempest.api.keyvalue import test
from tempest.common.utils.data_utils import rand_name
from tempest.common.utils.data_utils import rand_uuid


LOG = logging.getLogger(__name__)


class MagnetoDBTestCase(test.BotoTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBTestCase, cls).setUpClass()
        cls.os = tempest.clients.Manager()
        cls.client = cls.os.dynamodb_client
        # todo(yyekovenko) Research boto error handling verification approach
        #cls.ec = cls.ec2_error_code
        ######################################################################
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
        cls.throughput = {"ReadCapacityUnits": 1,
                          "WriteCapacityUnits": 1}
        ######################################################################

    def wait_for_table_active(self, table_name, timeout=120, interval=3):
        # TODO(yyekovenko) Add condition if creation failed?
        def check():
            resp = self.client.describe_table(table_name)
            if "Table" in resp and "TableStatus" in resp["Table"]:
                return resp["Table"]["TableStatus"] == "ACTIVE"

        return tempest.test.call_until_true(check, timeout, interval)

    def wait_for_table_deleted(self, table_name, timeout=120, interval=3):
        def check():
            return table_name not in self.client.list_tables()['TableNames']

        return tempest.test.call_until_true(check, timeout, interval)

    def build_spam_item(self, user_id, date, message_id,
                        from_header, to_header):
        return {
            "user_id": {"S": user_id},
            "date_message_id": {"S": date + "#" + message_id},
            "message_id": {"S": message_id},
            "from_header": {"S": from_header},
            "to_header": {"S": to_header},
        }

    # TODO(yyekovenko) Move to load tests
    def populate_spam_table(self, table_name, usercount, itemcount):

        dates = ['2013-12-0%sT16:00:00.000001' % i for i in range(1, 8)]
        from_headers = ["%s@mail.com" % rand_name() for _ in range(10)]
        to_headers = ["%s@mail.com" % rand_name() for _ in range(10)]
        emails = []
        # TODO bug workaround
        expected = {}

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
                self.client.put_item(table_name, item, expected=expected)
                # TODO can I just return resp['Attributes']?
        return new_items
