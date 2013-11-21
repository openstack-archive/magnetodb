# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2011 OpenStack Foundation
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

from magnetodb.openstack.common.middleware import context as context_middleware
from magnetodb.openstack.common import context


class ContextMiddleware(context_middleware.ContextMiddleware):

    def make_context(self, *args, **kwargs):
        """
        Create a context with the given arguments.
        """

        tenant_id = self.options.get('tenant_id', None)
        auth_token = self.options.get('auth_token', None)
        user_id = self.options.get('user_id', None)

        is_admin = self.options.get('is_admin', False)

        return context.RequestContext(auth_token=auth_token,
                                      user=user_id,
                                      tenant=tenant_id,
                                      is_admin=is_admin)

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
