# Copyright 2011 OpenStack Foundation
# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
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

import weakref

from oslo_context import context

from magnetodb.openstack.common import log as logging
from magnetodb import policy


LOG = logging.getLogger(__name__)


class RequestContext(context.RequestContext):
    """Security context and request information.

    Represents the user taking a given action within the system.

    """
    def __init__(self, roles=None, project_name=None, service_catalog=None,
                 **kwargs):
        """Initialize RequestContext.

        :param kwargs: Extra arguments that might be present, but we ignore
            because they possibly came in from older rpc messages.
        """

        super(RequestContext, self).__init__(**kwargs)

        self.roles = roles or []
        self.project_name = project_name

        if service_catalog:
            # Only include required parts of service_catalog
            self.service_catalog = [s for s in service_catalog
                                    if s.get('type') in ('object-store',)]
        else:
            # if list is empty or none
            self.service_catalog = []

        # We need to have RequestContext attributes defined
        # when policy.check_is_admin invokes request logging
        # to make it loggable.
        if self.is_admin is None:
            self.is_admin = policy.check_is_admin(self.roles)
        elif self.is_admin and 'admin' not in self.roles:
            self.roles.append('admin')

    def to_dict(self):
        default = super(RequestContext, self).to_dict()
        extra = {'project_name': self.project_name,
                 'roles': self.roles,
                 'service_catalog': self.service_catalog}
        return dict(default.items() + extra.items())

    def update_store_by_weakref(self):
        context._request_store.context = weakref.proxy(self)
