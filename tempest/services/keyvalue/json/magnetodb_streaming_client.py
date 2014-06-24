# Copyright 2014 Mirantis Inc.
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
from tempest import config


CONF = config.TempestConfig()


class MagnetoDBStreamingClientJSON(rest_client.RestClient):

    def __init__(self, config, user, password, auth_url, tenant_name=None,
                 auth_version='v2'):

        super(MagnetoDBStreamingClientJSON, self).__init__(
            config, user, password, auth_url, tenant_name, auth_version)

        self.service = CONF.magnetodb_streaming.service_type

    def upload_items(self, table_name, items):
        post_body = ''.join([json.dumps(item) + '\n' for item in items])
        return self.upload_raw_stream(table_name, post_body)

    def upload_raw_stream(self, table_name, stream):
        url = '/'.join(['data/tables', table_name, 'bulk_load'])
        resp, body = self.post(url, stream, self.headers)
        return resp, self._parse_resp(body)
