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

from oslo_serialization import jsonutils as json

from tempest.common import service_client
from tempest import config_magnetodb as config

CONF = config.CONF
service_client.CONF = CONF


class MagnetoDBStreamingClientJSON(service_client.ServiceClient):

    def upload_items(self, table_name, items):
        post_body = ''.join([json.dumps(item) + '\n' for item in items])
        return self.upload_raw_stream(table_name, post_body)

    def upload_raw_stream(self, table_name, stream):
        url = '/'.join(['tables', table_name, 'bulk_load'])
        resp, body = self.post(url, stream)
        return resp, self._parse_resp(body)

    def _parse_resp(self, body):
        return json.loads(body)
