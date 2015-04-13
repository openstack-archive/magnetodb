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

    def __init__(self, credentials=None, service=None):
        super(Manager, self).__init__(credentials, service)
        self.dynamodb_client = dynamodb_client.APIClientDynamoDB(
            self.identity_client
        )
        region = CONF.magnetodb.region
        self.magnetodb_client = (
            magnetodb_client.MagnetoDBClientJSON(
                self.auth_provider,
                CONF.magnetodb.catalog_type,
                region
            )
        )
        self.magnetodb_streaming_client = (
            magnetodb_streaming_client.MagnetoDBStreamingClientJSON(
                self.auth_provider,
                CONF.magnetodb_streaming.catalog_type,
                region
            )
        )
        self.magnetodb_management_client = (
            magnetodb_management_client.MagnetoDBManagementClientJSON(
                self.auth_provider,
                CONF.magnetodb_management.catalog_type,
                region
            )
        )
        self.magnetodb_monitoring_client = (
            magnetodb_monitoring_client.MagnetoDBMonitoringClientJSON(
                self.auth_provider,
                CONF.magnetodb_monitoring.catalog_type,
                region
            )
        )


class AltManager(Manager):

    def __init__(self, service=None):
        self.credentials = auth.get_credentials('alt_user')
        super(AltManager, self).__init__(self.credentials, service)


class AdminManager(Manager):

    def __init__(self, service=None):
        self.credentials = auth.get_credentials('identity_admin')
        super(AdminManager, self).__init__(
            self.credentials, service)
