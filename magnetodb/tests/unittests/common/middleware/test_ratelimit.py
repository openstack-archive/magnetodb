# Copyright 2014 Symantec Corporation.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import unittest
import mock
from magnetodb.common.middleware import rate_limit


class TestRateLimit(unittest.TestCase):
    """Unit tests for rate limiting."""

    _DEFAULT_TENANT_ID = "default_tenant"

    def test_get_tenant_id(self):
        expected_tenant_id = self._DEFAULT_TENANT_ID
        path = (rate_limit.MDB_DATA_API_URL_PREFIX +
                expected_tenant_id +
                "/tables")
        tenant_id = rate_limit.get_tenant_id(path)

        self.assertEqual(expected_tenant_id, tenant_id)

    def setUp(self):
        self.ratelimit_middleware = rate_limit.RateLimitMiddleware(
            None,
            dict(rps_per_tenant=1))
        self.request = mock.Mock()

    def test_process_request_get_tenant_id_from_path(self):
        self.request.path = (rate_limit.MDB_DATA_API_URL_PREFIX +
                             self._DEFAULT_TENANT_ID +
                             "/tables")
        self.ratelimit_middleware.process_request(self.request)
        self.assertIn(self._DEFAULT_TENANT_ID,
                      self.ratelimit_middleware.last_time)

    def test_process_request_get_tenant_id_from_header(self):
        self.request.path = None
        self.request.headers = {
            'X-Tenant-Id': self._DEFAULT_TENANT_ID
        }
        self.ratelimit_middleware.process_request(self.request)
        self.assertIn(self._DEFAULT_TENANT_ID,
                      self.ratelimit_middleware.last_time)
