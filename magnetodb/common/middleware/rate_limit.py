# Copyright 2014 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A middleware that limits amount of processed requests per tenant
up to configured value
"""

import re
import time

from magnetodb.common import exception
from magnetodb.common import wsgi
from magnetodb import notifier
from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class RateLimitMiddleware(wsgi.Middleware):
    def __init__(self, app, options):
        self.options = options
        self.last_time = {}

        try:
            self.rps_per_tenant = int(self.options.get('rps_per_tenant', 0))
        except Exception as e:
            LOG.error('Error defining request rate, {}', e)
            LOG.error('Rate limiting disabled')
            self.rps_per_tenant = 0

        super(RateLimitMiddleware, self).__init__(app)

    def process_request(self, req):
        tenant_id = self._get_tenant_id(req)

        now = time.time()
        prev = self.last_time.get(tenant_id, 0)

        if self.rps_per_tenant and now - prev < 1. / self.rps_per_tenant:
            LOG.debug('Request rate for tenant {} exceeded preconfigured'
                      ' limit {}. Request rejected.',
                      tenant_id, self.rps_per_tenant)
            notifier.get_notifier().info(
                {}, notifier.EVENT_TYPE_REQUEST_RATE_LIMITED, tenant_id)
            raise exception.RequestQuotaExceeded()

        self.last_time[tenant_id] = now

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)

    @staticmethod
    def _get_tenant_id(req):
        path = req.path

        LOG.debug('Request path: %s', path)

        tenant_id = None

        if re.match("^/v1/\w+", path):
            url_comp = path.split('/')
            tenant_id = url_comp[2]
        else:
            tenant_id = req.headers.get('X-Tenant-Id', None)

        return tenant_id
