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

import random
import string

from oslo_serialization import jsonutils as json

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.test import attr


class MagnetoDBStreamingTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBStreamingTest, cls).setUpClass()

    def random_name(self, length):
        return ''.join(random.choice(string.lowercase + string.digits)
                       for i in range(length))

    @attr(type='BW-1')
    def test_streaming(self):
        table_name = self.random_name(40)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        item_count = 100

        items = [
            self.build_smoke_item('forum{}'.format(n),
                                  'subject{}'.format(n),
                                  last_posted_by='Bulk{}'.format(n))
            for n in xrange(item_count)
        ]

        key = {self.hashkey: items[-1][self.hashkey],
               self.rangekey: items[-1][self.rangekey]}

        upload_status, upload_resp = self.streaming_client.upload_items(
            table_name, items)

        read_ = upload_resp['read']
        processed_ = upload_resp['processed']
        failed_ = upload_resp['failed']
        unprocessed_ = upload_resp['unprocessed']

        self.assertEqual(read_, item_count)
        self.assertEqual(processed_, item_count)
        self.assertEqual(failed_, 0)
        self.assertEqual(unprocessed_, 0)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)

        self.assertEqual('Bulk{}'.format(item_count - 1),
                         get_resp[1]['item']['last_posted_by']['S'])

    @attr(type='BW-2')
    def test_streaming_error_data(self):
        table_name = self.random_name(40)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        item_count = 100

        items = [
            self.build_smoke_item('forum{}'.format(n),
                                  'subject{}'.format(n),
                                  last_posted_by='Bulk{}'.format(n))
            for n in xrange(item_count)
        ]

        del items[-2]['subject']

        key = {self.hashkey: items[-3][self.hashkey],
               self.rangekey: items[-3][self.rangekey]}

        upload_status, upload_resp = self.streaming_client.upload_items(
            table_name, items)

        read_ = upload_resp['read']
        processed_ = upload_resp['processed']
        failed_ = upload_resp['failed']
        unprocessed_ = upload_resp['unprocessed']

        self.assertEqual(read_, item_count)
        self.assertEqual(processed_ + failed_ + unprocessed_, item_count)
        self.assertEqual(failed_, 1)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)

        self.assertEqual('Bulk{}'.format(item_count - 3),
                         get_resp[1]['item']['last_posted_by']['S'])

    @attr(type='BW-3')
    def test_streaming_nolf(self):
        table_name = self.random_name(40)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        item = self.build_smoke_item('forum{}'.format(0),
                                     'subject{}'.format(0),
                                     last_posted_by='Bulk{}'.format(0))

        key = {self.hashkey: item[self.hashkey],
               self.rangekey: item[self.rangekey]}

        stream = json.dumps(item)

        upload_status, upload_resp = self.streaming_client.upload_raw_stream(
            table_name, stream)

        read_ = upload_resp['read']
        processed_ = upload_resp['processed']
        failed_ = upload_resp['failed']
        unprocessed_ = upload_resp['unprocessed']

        self.assertEqual(read_, 1)
        self.assertEqual(processed_, 1)
        self.assertEqual(failed_, 0)
        self.assertEqual(unprocessed_, 0)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)

        self.assertEqual('Bulk{}'.format(0),
                         get_resp[1]['item']['last_posted_by']['S'])

    @attr(type='BW-4')
    def test_streaming_bad_stream(self):
        table_name = self.random_name(40)
        self._create_test_table(self.smoke_attrs + self.index_attrs,
                                table_name,
                                self.smoke_schema,
                                wait_for_active=True)

        item_count = 100

        items = [
            self.build_smoke_item('forum{}'.format(n),
                                  'subject{}'.format(n),
                                  last_posted_by='Bulk{}'.format(n))
            for n in xrange(item_count)
        ]

        key = {self.hashkey: items[0][self.hashkey],
               self.rangekey: items[0][self.rangekey]}

        stream = ''.join([json.dumps(item) + '\n' for item in items])
        stream = stream[:len(stream)/2]

        upload_status, upload_resp = self.streaming_client.upload_raw_stream(
            table_name, stream)

        read_ = upload_resp['read']
        processed_ = upload_resp['processed']
        failed_ = upload_resp['failed']
        unprocessed_ = upload_resp['unprocessed']

        self.assertEqual(processed_ + failed_ + unprocessed_, read_)

        attributes_to_get = ['last_posted_by']
        get_resp = self.client.get_item(table_name,
                                        key,
                                        attributes_to_get,
                                        True)

        self.assertEqual('Bulk{}'.format(0),
                         get_resp[1]['item']['last_posted_by']['S'])
