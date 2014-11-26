# Copyright 2014 Mirantis Inc.
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

from magnetodb.api.openstack.v1 import utils
from magnetodb.api import validation
from magnetodb.common import probe


class CreateRestoreJobController(object):
    """ Creates a restore job. """

    @probe.Probe(__name__)
    def create_restore_job(self, req, body, project_id, table_id):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id

        with probe.Probe(__name__ + '.validation'):
            validation.validate_object(body, "body")

            validation.validate_unexpected_props(body, "body")

        return {}
