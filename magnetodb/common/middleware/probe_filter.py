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

from oslo_middleware import base as wsgi

from magnetodb.common import probe

from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class ProbeFilter(wsgi.Middleware):
    """Middleware that measures request proccessing time.

    Put this filter to api-paste.ini before the filter you want to probe
    to turn on time measuring. Can be used several times with
    different filters.
    """
    def __init__(self, app, options):
        self.options = options
        self.probe = probe.Probe(app.__class__)
        super(ProbeFilter, self).__init__(app)

    def process_request(self, req):
        with self.probe:
            return req.get_response(self.application)

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
