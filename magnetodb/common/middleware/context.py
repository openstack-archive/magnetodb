# Copyright 2014 Symantec Corporation
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

import webob.exc

from oslo_serialization import jsonutils as json

from oslo_middleware import base as wsgi

from magnetodb import context

from magnetodb.i18n import _


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
        auth_token = kwargs.get('auth_token', None)
        auth_token = auth_token or self.options.get('auth_token', None)
        user_id = kwargs.get('user_id', None)
        user_id = user_id or self.options.get('user_id', None)
        service_catalog = kwargs.get('service_catalog', None)

        is_admin = self.options.get('is_admin', False)
        roles = kwargs.get('roles', None)
        if roles:
            roles = roles.split(',')

        return context.RequestContext(auth_token=auth_token,
                                      user=user_id,
                                      tenant=tenant_name,
                                      is_admin=is_admin,
                                      roles=roles,
                                      service_catalog=service_catalog)

    def process_request(self, req):
        """
        Extract any authentication information in the request and
        construct an appropriate context from it.
        """
        # Use the default empty context, with admin turned on for
        # backwards compatibility
        user_id = req.headers.get('X-User-Id', None)
        tenant_id = req.headers.get('X-Tenant-Id', None)
        roles = req.headers.get('X-Roles', None)

        service_catalog = None
        if req.headers.get('X_SERVICE_CATALOG') is not None:
            try:
                catalog_header = req.headers.get('X_SERVICE_CATALOG')
                service_catalog = json.loads(catalog_header)
            except ValueError:
                raise webob.exc.HTTPInternalServerError(
                    explanation=_('Invalid service catalog json.'))

        auth_token = req.headers.get('X-Auth-Token', None)
        req.environ['magnetodb.context'] = self.make_context(
            is_admin=True,
            auth_token=auth_token,
            user_id=user_id,
            tenant_id=tenant_id,
            roles=roles,
            service_catalog=service_catalog
        )

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)

    @staticmethod
    def tenant_id_to_keyspace_name(tenant_id):
        return filter(lambda x: x != '-', tenant_id)
