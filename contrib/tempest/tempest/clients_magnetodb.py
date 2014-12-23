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

from tempest import auth
from tempest import clients
from tempest.services.keyvalue import dynamodb_client
from tempest.services.keyvalue.json import magnetodb_client
from tempest.services.keyvalue.json import magnetodb_streaming_client
from tempest.services.keyvalue.json import magnetodb_management_client
from tempest.services.keyvalue.json import magnetodb_monitoring_client

from tempest import config_magnetodb as config


CONF = config.CONF


class Manager(clients.Manager):

    def __init__(self, credentials=None, interface='json', service=None):
        super(Manager, self).__init__(credentials, interface, service)
        auth_provider = self.get_auth_provider(self.credentials)
        creds = auth_provider.credentials
        auth_url = CONF.identity.uri
        if CONF.identity.auth_version == 'v3':
            auth_url = CONF.identity.uri_v3
        ks_creds = creds.username, creds.password, auth_url, creds.tenant_name
        self.dynamodb_client = dynamodb_client.APIClientDynamoDB(*ks_creds)
        if interface == 'json':
            self.magnetodb_client = (
                magnetodb_client.MagnetoDBClientJSON(auth_provider)
            )
            self.magnetodb_streaming_client = (
                magnetodb_streaming_client.MagnetoDBStreamingClientJSON(
                    auth_provider)
            )
            self.magnetodb_management_client = (
                magnetodb_management_client.MagnetoDBManagementClientJSON(
                    auth_provider)
            )
            self.magnetodb_monitoring_client = (
                magnetodb_monitoring_client.MagnetoDBMonitoringClientJSON(
                    auth_provider)
            )


class AltManager(Manager):
    def __init__(self, interface='json', service=None):
        self.credentials = auth.get_credentials('alt_user')
        super(AltManager, self).__init__(self.credentials, interface, service)


class AdminManager(Manager):
    def __init__(self, interface='json', service=None):
        self.credentials = auth.get_credentials('identity_admin')
        super(AdminManager, self).__init__(
            self.credentials, interface, service)
