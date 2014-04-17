# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import ConfigParser
import contextlib
import types
import urlparse

from boto.dynamodb2.layer1 import DynamoDBConnection
from tempest import exceptions

# TODO(yyekovenko) Added to simplify/speed-up debugging
DynamoDBConnection.NumberRetries = 0

import boto


class BotoClientBase(object):

    ALLOWED_METHODS = set()

    def __init__(self, config,
                 username=None, password=None,
                 auth_url=None, tenant_name=None,
                 *args, **kwargs):

        self.connection_timeout = str(config.boto.http_socket_timeout)
        self.num_retries = str(config.boto.num_retries)
        self.build_timeout = config.boto.build_timeout
        self.ks_cred = {"username": username,
                        "password": password,
                        "auth_url": auth_url,
                        "tenant_name": tenant_name}

    def _keystone_aws_get(self):
        import keystoneclient.v2_0.client

        keystone = keystoneclient.v2_0.client.Client(**self.ks_cred)
        ec2_cred_list = keystone.ec2.list(keystone.auth_user_id)
        ec2_cred = None
        for cred in ec2_cred_list:
            if cred.tenant_id == keystone.auth_tenant_id:
                ec2_cred = cred
                break
        else:
            ec2_cred = keystone.ec2.create(keystone.auth_user_id,
                                           keystone.auth_tenant_id)
        if not all((ec2_cred, ec2_cred.access, ec2_cred.secret)):
            raise exceptions.NotFound("Unable to get access and secret keys")
        return ec2_cred

    def _config_boto_timeout(self, timeout, retries):
        try:
            boto.config.add_section("Boto")
        except ConfigParser.DuplicateSectionError:
            pass
        boto.config.set("Boto", "http_socket_timeout", timeout)
        boto.config.set("Boto", "num_retries", retries)

    def __getattr__(self, name):
        """Automatically creates methods for the allowed methods set."""
        if name in self.ALLOWED_METHODS:
            def func(self, *args, **kwargs):
                with contextlib.closing(self.get_connection()) as conn:
                    return getattr(conn, name)(*args, **kwargs)

            func.__name__ = name
            setattr(self, name, types.MethodType(func, self, self.__class__))
            setattr(self.__class__, name,
                    types.MethodType(func, None, self.__class__))
            return getattr(self, name)
        else:
            raise AttributeError(name)

    def get_connection(self):
        self._config_boto_timeout(self.connection_timeout, self.num_retries)
        if not all((self.connection_data["aws_access_key_id"],
                   self.connection_data["aws_secret_access_key"])):
            if all(self.ks_cred.itervalues()):
                ec2_cred = self._keystone_aws_get()
                self.connection_data["aws_access_key_id"] = \
                    ec2_cred.access
                self.connection_data["aws_secret_access_key"] = \
                    ec2_cred.secret
            else:
                raise exceptions.InvalidConfiguration(
                    "Unable to get access and secret keys")
        return self.connect_method(**self.connection_data)


class APIClientDynamoDB(BotoClientBase):

    def connect_method(self, *args, **kwargs):
        return DynamoDBConnection(**kwargs)

    def __init__(self, config, *args, **kwargs):
        super(APIClientDynamoDB, self).__init__(config, *args, **kwargs)
        aws_access = config.boto.aws_access
        aws_secret = config.boto.aws_secret
        region = config.boto.region
        purl = urlparse.urlparse(config.boto.magnetodb_url)
        port = purl.port
        if port is None:
            if purl.scheme is not "https":
                port = 80
            else:
                port = 443
        else:
            port = int(port)
        self.connection_data = {"aws_access_key_id": aws_access,
                                "aws_secret_access_key": aws_secret,
                                "is_secure": purl.scheme == "https",
                                "host": purl.hostname,
                                "port": port,
                                "region": region
                                }

    ALLOWED_METHODS = set(('create_table', 'delete_table', 'list_tables',
                           'update_table', 'describe_table',
                           'batch_get_item', 'batch_write_item',
                           'put_item', 'update_item', 'delete_item',
                           'get_item', 'query', 'scan'))
