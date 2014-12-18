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

import json

from tempest.common import rest_client
from tempest import config_magnetodb as config

CONF = config.CONF
rest_client.CONF = CONF


class MagnetoDBMonitoringClientJSON(rest_client.RestClient):

    def __init__(self, *args, **kwargs):
        super(MagnetoDBMonitoringClientJSON, self).__init__(*args, **kwargs)
        self.service = CONF.magnetodb_monitoring.catalog_type

    def get_all_metrics(self, table_name):
        url = '/'.join([self.tenant_id, 'tables', table_name])
        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def _parse_resp(self, body):
        return json.loads(body)
