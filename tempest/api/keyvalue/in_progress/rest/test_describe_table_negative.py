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

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest import exceptions


class MagnetoDBDescribeTableNegativeTestCase(MagnetoDBTestCase):

    def __init__(self, *args, **kwargs):
        super(MagnetoDBDescribeTableNegativeTestCase,
              self).__init__(*args, **kwargs)
        self.tname = rand_name().replace('-', '')

    def test_describe_table_nonexistent(self):
        tname = rand_name().replace('-', '')
        self.assertRaises(exceptions.NotFound, self.client.describe_table,
                          tname)

    def test_describe_table_empty_name(self):
        self.assertRaises(exceptions.BadRequest, self.client.describe_table,
                          table_name="")

    def test_describe_table_short_name(self):
        self.assertRaises(exceptions.BadRequest, self.client.describe_table,
                          table_name="aa")

    def test_describe_table_too_long_name(self):
        self.assertRaises(exceptions.BadRequest, self.client.describe_table,
                          table_name="a" * 256)
