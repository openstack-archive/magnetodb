# Copyright 2014 Symantec Corporation
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

from magnetodb import api
from magnetodb.api.openstack.v1 import parser
from magnetodb.api import validation
from magnetodb.common import probe
from magnetodb.common.utils import request_context_decorator
from magnetodb.openstack.common import log as logging
from magnetodb import storage

LOG = logging.getLogger(__name__)


@api.enforce_policy("mdb:describe_table")
@probe.Probe(__name__)
@request_context_decorator.request_type("describe_table")
def describe_table(req, project_id, table_name):
    """Returns information about the table."""

    validation.validate_table_name(table_name)

    table_meta = storage.describe_table(project_id, table_name)

    url = req.path_url
    bookmark = req.path_url

    result = {
        parser.Props.TABLE: {
            parser.Props.ATTRIBUTE_DEFINITIONS: (
                parser.Parser.format_attribute_definitions(
                    table_meta.schema.attribute_type_map)),

            parser.Props.CREATION_DATE_TIME: table_meta.creation_date_time,
            parser.Props.ITEM_COUNT: 0,

            parser.Props.KEY_SCHEMA: (
                parser.Parser.format_key_schema(
                    table_meta.schema.key_attributes)),

            parser.Props.TABLE_ID: str(table_meta.id),
            parser.Props.TABLE_NAME: table_name,

            parser.Props.TABLE_STATUS: (
                parser.Parser.format_table_status(table_meta.status)),

            parser.Props.TABLE_SIZE_BYTES: 0,

            parser.Props.LINKS: [
                {
                    parser.Props.HREF: url,
                    parser.Props.REL: parser.Values.SELF
                },
                {
                    parser.Props.HREF: bookmark,
                    parser.Props.REL: parser.Values.BOOKMARK
                }
            ]
        }
    }

    if table_meta.schema.index_def_map:
        table_def = result[parser.Props.TABLE]
        table_def[parser.Props.LOCAL_SECONDARY_INDEXES] = (
            parser.Parser.format_local_secondary_indexes(
                table_meta.schema.key_attributes[0],
                table_meta.schema.index_def_map
            )
        )

    return result
