# Copyright 2014 Symantec Corporation
# Copyright 2013 Mirantis Inc.
# Copyright 2012 OpenStack Foundation
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

from tempest import config
from tempest import exceptions
from tempest.openstack.common import log as logging
from tempest.services import botoclients
from tempest.services.keyvalue.json.magnetodb_client import MagnetoDBClientJSON
from tempest.services.keyvalue.json import magnetodb_streaming_client
from tempest.services.keyvalue.json import magnetodb_monitoring_client
from tempest.services.keyvalue.json import magnetodb_management_client

LOG = logging.getLogger(__name__)


class Manager(object):

    """
    Top level manager for OpenStack Compute clients
    """

    def __init__(self, username=None, password=None, tenant_name=None,
                 interface='json'):
        """
        We allow overriding of the credentials used within the various
        client classes managed by the Manager object. Left as None, the
        standard username/password/tenant_name is used.

        :param username: Override of the username
        :param password: Override of the password
        :param tenant_name: Override of the tenant name
        """
        self.config = config.TempestConfig()

        # If no creds are provided, we fall back on the defaults
        # in the config file for the Compute API.
        # Using admin_username to create ec2 creds.
        self.username = username or self.config.identity.admin_username
        self.password = password or self.config.identity.password
        self.tenant_name = tenant_name or self.config.identity.tenant_name

        if None in (self.username, self.password, self.tenant_name):
            msg = ("Missing required credentials. "
                   "username: %(u)s, password: %(p)s, "
                   "tenant_name: %(t)s" %
                   {'u': username, 'p': password, 't': tenant_name})
            raise exceptions.InvalidConfiguration(msg)

        self.auth_url = self.config.identity.uri_v3
        self.auth_version = 'v3'

        client_args = (self.config, self.username, self.password,
                       self.auth_url, self.tenant_name, self.auth_version)

        # common clients
        self.dynamodb_client = botoclients.APIClientDynamoDB(*client_args)
        self.magnetodb_client = MagnetoDBClientJSON(*client_args)
        self.magnetodb_monitoring_client = (
            magnetodb_monitoring_client.MagnetoDBMonitoringClientJSON(
                *client_args))
        self.magnetodb_streaming_client = (
            magnetodb_streaming_client.MagnetoDBStreamingClientJSON(
                *client_args))
        self.magnetodb_management_client = (
            magnetodb_management_client.MagnetoDBManagementClientJSON(
                *client_args))


class AltManager(Manager):
    """
    Manager object that uses the alt_XXX credentials for its
    managed client objects
    """

    def __init__(self):
        conf = config.TempestConfig()
        super(AltManager, self).__init__(conf.identity.alt_username,
                                         conf.identity.alt_password,
                                         conf.identity.alt_tenant_name)
