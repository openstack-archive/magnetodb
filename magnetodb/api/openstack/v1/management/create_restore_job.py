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

import uuid

from magnetodb import storage
from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb.api import validation
from magnetodb.common import probe


@probe.Probe(__name__)
def create_restore_job(req, project_id, table_name):
    """Creates a restore job."""

    utils.check_project_id(project_id)

    with probe.Probe(__name__ + '.validation'):
        validation.validate_table_name(table_name)

        body = req.json_body
        validation.validate_object(body, "body")

        backup_id = body.pop(parser.Props.BACKUP_ID, None)
        if backup_id:
            backup_id = uuid.UUID(backup_id)

        source = body.pop(parser.Props.SOURCE, None)

        validation.validate_unexpected_props(body, "body")

    restore_job = storage.create_restore_job(
        project_id, table_name, backup_id, source
    )

    href_prefix = req.path_url
    response = parser.Parser.format_restore_job(restore_job, href_prefix)

    return response
