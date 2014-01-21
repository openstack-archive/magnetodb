# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 OpenStack Foundation
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

from tempest.api.keyvalue.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.test import attr


class MagnetoDBScanTest(MagnetoDBTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBScanTest, cls).setUpClass()
        cls.tname = rand_name().replace('-', '')
        cls.client.create_table(cls.smoke_attrs,
                                cls.tname,
                                cls.smoke_schema,
                                cls.smoke_throughput)
        cls.addResourceCleanUp(cls.client.delete_table, cls.tname)

    @attr(type='smoke')
    def test_scan(self):
        item = self.build_smoke_item('forum1', 'subject2')
        self.client.put_item(self.tname, item)
        resp = self.client.scan(table_name=self.tname)
        self.assertTrue(resp['Count'] > 0)
