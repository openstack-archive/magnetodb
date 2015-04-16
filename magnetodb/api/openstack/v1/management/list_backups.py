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
from magnetodb.api import validation
from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb.common import probe


@probe.Probe(__name__)
def list_backups(req, project_id, table_name):
    """List the backups."""

    utils.check_project_id(project_id)

    with probe.Probe(__name__ + '.validation'):
        validation.validate_table_name(table_name)

        params = req.params.copy()

        exclusive_start_backup_id = params.pop(
            parser.Props.EXCLUSIVE_START_BACKUP_ID, None)

        if exclusive_start_backup_id:
            exclusive_start_backup_id = uuid.UUID(
                exclusive_start_backup_id
            )

        limit = params.pop(parser.Props.LIMIT, None)
        if limit:
            limit = validation.validate_integer(limit, parser.Props.LIMIT,
                                                min_val=0)

    backups = storage.list_backups(
        project_id, table_name, exclusive_start_backup_id, limit)

    response = {}

    if backups and limit == len(backups):
        response[parser.Props.LAST_EVALUATED_BACKUP_ID] = (
            backups[-1].id.hex)

    self_link_prefix = req.path_url

    response[parser.Props.BACKUPS] = [
        parser.Parser.format_backup(backup, self_link_prefix)
        for backup in backups
    ]

    return response
