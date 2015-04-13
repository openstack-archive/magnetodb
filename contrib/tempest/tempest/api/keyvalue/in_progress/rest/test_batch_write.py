# Copyright 2014 Mirantis Inc.
# Copyright 2014 Symantec Corporation
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

from tempest_lib import exceptions

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBBatchWriteTest(MagnetoDBTestCase):

    def setUp(self):
        super(MagnetoDBBatchWriteTest, self).setUp()
        self.tname = rand_name(self.table_prefix).replace('-', '')

    @attr(type=['BWI-17'])
    def test_batch_write_put_n_empty_value(self):
        self._create_test_table(self.build_x_attrs('S'), self.tname,
                                self.smoke_schema,
                                wait_for_active=True)
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'N', ''))
        request_body = {'request_items': {self.tname: [{'put_request':
                                                        {'item': item}}]}}
        with self.assertRaises(exceptions.BadRequest):
            self.client.batch_write_item(request_body)

    @attr(type=['BWI-54_1'])
    def test_batch_write_too_short_tname(self):
        tname = 'qq'
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {tname: [{'put_request':
                                                   {'item': item}}]}}

        with self.assertRaises(exceptions.BadRequest):
            self.client.batch_write_item(request_body)

    @attr(type=['BWI-54_2'])
    def test_batch_write_too_long_tname(self):
        tname = 'q' * 256
        item = self.build_x_item('S', 'forum1', 'subject2',
                                 ('message', 'S', 'message text'))
        request_body = {'request_items': {tname: [{'put_request':
                                                   {'item': item}}]}}

        with self.assertRaises(exceptions.BadRequest):
            self.client.batch_write_item(request_body)
