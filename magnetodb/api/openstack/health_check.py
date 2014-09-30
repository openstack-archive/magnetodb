# Copyright 2014 Symantec Corporation
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


from keystoneclient.generic import client

from magnetodb import storage
from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)


class HealthCheckController(object):
    """ Controller for health check request. """

    def __init__(self, auth_uri=''):
        super(HealthCheckController, self).__init__()
        self.auth_uri = auth_uri
        self.keystoneclient = client.Client()

    def health_check(self, req):
        storage.health_check()
        self.keystoneclient.discover(self.auth_uri)
        return
