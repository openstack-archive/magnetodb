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
from swiftclient import exceptions as swift_exceptions

from magnetodb.common import config
from magnetodb.common import exception
from magnetodb.openstack.common import log as logging

CONF = config.CONF
LOG = logging.get_logger(__name__)


class SwiftAPI(external_storage.ExternalStorageAPI):

    def __init__(self, url=''):
        super(SwiftAPI, self).__init__()
        self.url = url

    def put_data(self, context, container_name, object_name, data):
        if not self.url:
            self.url = self._get_endpoint_url(context)
        token = context.auth_token
        try:
            swift_client.put_container(self.url, token, container_name)
            swift_client.put_object(
                self.url, token, container_name, object_name, data
            )
        except swift_exceptions.ClientException as e:
            LOG.error(e)
            raise exception.DataUploadError(
                container_name=container_name,
                object_name=object_name
            )
        location = '/'.join([self.url, container_name])
        return location

    def get_data(self, context, container_name, object_name,
                 chunk_size=1024):
        if not self.url:
            self.url = self._get_endpoint_url(context)
        token = context.auth_token
        try:
            return swift_client.get_object(
                self.url, token, container_name, object_name,
                resp_chunk_size=chunk_size
            )[1]
        except swift_exceptions.ClientException as e:
            LOG.error(e)
            if e.http_status == 404:
                raise exception.ContainerNotExists(
                    container_name=container_name
                )
            else:
                raise exception.DataDownloadError(
                    container_name=container_name,
                    object_name=object_name
                )

    def delete_data(self, context, container_name):
        if not self.url:
            self.url = self._get_endpoint_url(context)
        token = context.auth_token
        try:
            headers, info = swift_client.get_container(
                self.url, token, container_name
            )
            for obj in info:
                try:
                    swift_client.delete_object(
                        self.url, token, container_name, obj['name']
                    )
                except swift_exceptions.ClientException as e:
                    LOG.error(e)
                    if e.http_status != 404:
                        raise
            swift_client.delete_container(self.url, token, container_name)
        except swift_exceptions.ClientException as e:
            LOG.error(e)
            if e.http_status == 404:
                raise exception.ContainerNotExists(
                    container_name=container_name
                )
            else:
                raise exception.ContainerDeletionError(
                    container_name=container_name
                )

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
