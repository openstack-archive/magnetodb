#    Copyright 2012 OpenStack Foundation
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

import routes

from magnetodb.openstack.common import wsgi
from magnetodb.api import controller as api_controller


class MagnetoDBAppFactrory(wsgi.Router):
    """API"""
    def __init__(self):
        mapper = routes.Mapper()
        super(MagnetoDBAppFactrory, self).__init__(mapper)

        api_app = wsgi.Resource(api_controller.ApiController())
        mapper.connect("/", controller=api_app, conditions={'method': 'PUT'},
                       action="process_request")


def app_factory(global_conf, **local_conf):
    return MagnetoDBAppFactrory()
