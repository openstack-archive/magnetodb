# Copyright 2014 Symantec Corporation
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

from tempest.common import rest_client
from tempest import config


CONF = config.TempestConfig()


class MagnetoDBMonitoringClientJSON(rest_client.RestClient):

    def __init__(self, config, user, password, auth_url, tenant_name=None,
                 auth_version='v2'):

        super(MagnetoDBMonitoringClientJSON, self).__init__(
            config, user, password, auth_url, tenant_name, auth_version)

        self.service = CONF.magnetodb_monitoring.service_type

    def get_all_metrics(self, table_name):
        url = '/'.join([self.tenant_id, 'tables', table_name])
        resp, body = self.get(url, self.headers)
        return resp, self._parse_resp(body)
