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

from tempest.api.keyvalue.boto_base.base import MagnetoDBTestCase
from tempest_lib.common.utils.data_utils import rand_name
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


class MagnetoDBUpdateTest(MagnetoDBTestCase):
    @classmethod
    def setUpClass(cls):
        super(MagnetoDBUpdateTest, cls).setUpClass()

    def _create_table_for_test(self, wait):
        self.table_name = rand_name(self.table_prefix).replace('-', '')
        self.client.create_table(self.smoke_attrs + self.index_attrs,
                                 self.table_name,
                                 self.smoke_schema,
                                 self.smoke_throughput,
                                 self.smoke_lsi,
                                 self.smoke_gsi)
        if wait:
            self.wait_for_table_active(self.table_name)
        self.addResourceCleanUp(self.client.delete_table, self.table_name)
