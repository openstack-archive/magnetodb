# Copyright 2015 Symantec Corporation
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

from keystoneclient import service_catalog
from swiftclient import client as swift_client

from magnetodb.common import config
from magnetodb.common import exception
from magnetodb.common import external_storage

CONF = config.CONF


class SwiftAPI(external_storage.ExternalStorageAPI):

    def __init__(self, url=''):
        super(SwiftAPI, self).__init__()
        self.url = url

    def export_data(self, context, location, name, data):
        if not self.url:
            self.url = self._get_endpoint_url(context)
        token = context.auth_token
        swift_client.put_container(self.url, token, location)
        swift_client.put_object(self.url, token, location, name, data)

    def import_data(self, location):
        return NotImplemented

    def _get_endpoint_url(self, context):
        token_info = context.token_info['token']
        if service_catalog.ServiceCatalogV2.is_valid(token_info):
            catalog = service_catalog.ServiceCatalogV2(context.token_info)
        elif service_catalog.ServiceCatalogV3.is_valid(token_info):
            catalog = service_catalog.ServiceCatalogV3(
                context.auth_token, token_info
            )
        else:
            raise exception.MagnetoError('V2 or V3 tokens are supported')
        return catalog.url_for(service_type='object-store')
