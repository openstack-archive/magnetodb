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

from oslo_serialization import jsonutils as json

from tempest.common import service_client
from tempest import config_magnetodb as config

CONF = config.CONF
service_client.CONF = CONF


class MagnetoDBMonitoringClientJSON(service_client.ServiceClient):

    def get_all_metrics(self, table_name):
        url = '/'.join(['projects', self.tenant_id, 'tables', table_name])
        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def get_all_project_metrics(self):
        url = '/'.join(['projects', self.tenant_id])
        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def get_all_project_tables_metrics(self):
        url = 'projects'
        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def _parse_resp(self, body):
        return json.loads(body)
