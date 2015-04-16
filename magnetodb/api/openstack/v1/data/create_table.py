# Copyright 2014 Mirantis Inc.
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

from magnetodb import api
from magnetodb.api.openstack.v1 import parser
from magnetodb.api import validation
from magnetodb.common import exception
from magnetodb.common import probe
from magnetodb.common.utils import request_context_decorator
from magnetodb.i18n import _
from magnetodb.openstack.common import log as logging
from magnetodb import storage
from magnetodb.storage import models

LOG = logging.getLogger(__name__)


@api.enforce_policy("mdb:create_table")
@probe.Probe(__name__)
@request_context_decorator.request_type("create_table")
def create_table(req, project_id):
    """The CreateTable operation adds a new table.
    Table names must be unique within each tenant.
    """

    with probe.Probe(__name__ + '.validate'):
        body = req.json_body
        validation.validate_object(body, "body")

        table_name = body.pop(parser.Props.TABLE_NAME, None)
        validation.validate_table_name(table_name)

        # parse table attributes
        attribute_definitions_json = body.pop(
            parser.Props.ATTRIBUTE_DEFINITIONS, None
        )
        validation.validate_list_of_objects(
            attribute_definitions_json, parser.Props.ATTRIBUTE_DEFINITIONS
        )

        attribute_definitions = parser.Parser.parse_attribute_definitions(
            attribute_definitions_json
        )

        # parse table key schema
        key_attrs_json = body.pop(parser.Props.KEY_SCHEMA, None)
        validation.validate_list(key_attrs_json, parser.Props.KEY_SCHEMA)

        key_attrs = parser.Parser.parse_key_schema(key_attrs_json)

        # parse table indexed field list
        lsi_defs_json = body.pop(
            parser.Props.LOCAL_SECONDARY_INDEXES, None
        )

        if lsi_defs_json:
            validation.validate_list_of_objects(
                lsi_defs_json, parser.Props.LOCAL_SECONDARY_INDEXES
            )

            index_def_map = parser.Parser.parse_local_secondary_indexes(
                lsi_defs_json
            )
        else:
            index_def_map = {}

        # validate the uniqueness of table and its indices' key schema
        range_keys = []
        if len(key_attrs) > 1:
            range_keys.append(key_attrs[1])
        else:
            # table has hash type primary key
            if len(index_def_map) > 0:
                raise exception.ValidationError(
                    _("Table without range key in primary key schema "
                      "can not have indices"))
        for index in index_def_map.values():
            range_keys.append(index.alt_range_key_attr)
        try:
            validation.validate_set(range_keys, "key_schema")
        except exception.ValidationError:
            raise exception.ValidationError(
                _("Table and its indices must have unique key schema"))

        validation.validate_unexpected_props(body, "body")
    # prepare table_schema structure
    table_schema = models.TableSchema(
        attribute_definitions, key_attrs, index_def_map)

    table_meta = storage.create_table(project_id, table_name, table_schema)

    url = req.path_url + "/" + table_name
    bookmark = req.path_url + "/" + table_name

    result = {
        parser.Props.TABLE_DESCRIPTION: {
            parser.Props.ATTRIBUTE_DEFINITIONS: (
                parser.Parser.format_attribute_definitions(
                    table_meta.schema.attribute_type_map
                )
            ),
            parser.Props.CREATION_DATE_TIME: table_meta.creation_date_time,
            parser.Props.ITEM_COUNT: 0,
            parser.Props.KEY_SCHEMA: (
                parser.Parser.format_key_schema(
                    table_meta.schema.key_attributes
                )
            ),
            parser.Props.TABLE_ID: str(table_meta.id),
            parser.Props.TABLE_NAME: table_name,
            parser.Props.TABLE_STATUS: (
                parser.Parser.format_table_status(table_meta.status)
            ),
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
        table_def = result[parser.Props.TABLE_DESCRIPTION]
        table_def[parser.Props.LOCAL_SECONDARY_INDEXES] = (
            parser.Parser.format_local_secondary_indexes(
                table_meta.schema.key_attributes[0],
                table_meta.schema.index_def_map
            )
        )

    return result
