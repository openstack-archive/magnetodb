# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

import re
from magnetodb.openstack.common import exception as openstack_exception
from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)


def safe_fmt_string(text):
    return re.sub(r'%([0-9]+)', r'\1', text)


class MagnetoError(openstack_exception.OpenstackException):
    """Base exception that all custom trove app exceptions inherit from."""
    internal_message = None

    def __init__(self, message=None, **kwargs):
        if message is not None:
            self.message = message
        if self.internal_message is not None:
            try:
                LOG.error(safe_fmt_string(self.internal_message) % kwargs)
            except Exception:
                LOG.error(self.internal_message)
        self.message = safe_fmt_string(self.message)
        super(MagnetoError, self).__init__(**kwargs)


class ValidationException(MagnetoError):
    pass


class ServiceUnavailableException(MagnetoError):
    pass


class BackendInteractionException(MagnetoError):
    pass

