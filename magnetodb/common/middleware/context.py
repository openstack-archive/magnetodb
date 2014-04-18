# Copyright 2013 Mirantis Inc.
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

from magnetodb.common import wsgi
from magnetodb.openstack.common import context


class ContextMiddleware(wsgi.Middleware):
    def __init__(self, app, options):
        self.options = options
        super(ContextMiddleware, self).__init__(app)

    def make_context(self, *args, **kwargs):
        """
        Create a context with the given arguments.
        """

        tenant_id = kwargs.get('tenant_id', None)
        tenant_id = tenant_id or self.options.get('tenant_id', None)
        tenant_name = self.tenant_id_to_keyspace_name(tenant_id)
        auth_token = self.options.get('auth_token', None)
        user_id = kwargs.get('user_id', None)
        user_id = user_id or self.options.get('user_id', None)

        is_admin = self.options.get('is_admin', False)

        return context.RequestContext(auth_token=auth_token,
                                      user=user_id,
                                      tenant=tenant_name,
                                      is_admin=is_admin)

    def process_request(self, req):
        """
        Extract any authentication information in the request and
        construct an appropriate context from it.
        """
        # Use the default empty context, with admin turned on for
        # backwards compatibility
        user_id = req.headers.get('X-Auth-User', None)
        tenant_id = req.headers.get('X-Tenant-Id', None)
        req.context = self.make_context(is_admin=True,
                                        user_id=user_id,
                                        tenant_id=tenant_id)

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)

    @staticmethod
    def tenant_id_to_keyspace_name(tenant_id):
        return filter(lambda x: x != '-', tenant_id)
