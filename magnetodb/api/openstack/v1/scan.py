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
import jsonschema

from magnetodb import storage

from magnetodb.openstack.common.log import logging

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils

LOG = logging.getLogger(__name__)


class ScanController(object):
    schema = {
        "properties": {
            parser.Props.ATTRIBUTES_TO_GET: {
                "type": "array",
                "items": parser.Types.ATTRIBUTE_NAME
            },

            parser.Props.EXCLUSIVE_START_KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },

            parser.Props.LIMIT: {
                "type": "integer",
                "minimum": 0,
            },

            parser.Props.SCAN_FILTER: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "type": "object",
                        "properties": {
                            parser.Props.ATTRIBUTE_VALUE_LIST: {
                                "type": "array",
                                "items": parser.Types.ITEM_VALUE
                            },
                            parser.Props.COMPARISON_OPERATOR: (
                                parser.Types.SCAN_OPERATOR
                            )
                        }
                    }
                }
            },

            parser.Props.SEGMENT: {
                "type": "integer",
                "minimum": 0,
            },

            parser.Props.SELECT: parser.Types.SELECT,

            parser.Props.TOTAL_SEGMENTS: {
                "type": "integer",
                "minimum": 1,
                "maximum": 4096,
            }
        }
    }

    def scan(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)

        req.context.tenant = project_id

        # TODO ikhudoshyn: table_name may be index name

        attrs_to_get = body.get(parser.Props.ATTRIBUTES_TO_GET)

        select = body.get(parser.Props.SELECT)

        select_type = parser.Parser.parse_select_type(select, attrs_to_get)

        limit = body.get(parser.Props.LIMIT)

        exclusive_start_key = body.get(parser.Props.EXCLUSIVE_START_KEY)

        exclusive_start_key = parser.Parser.parse_item_attributes(
            exclusive_start_key) if exclusive_start_key else None

        scan_filter = body.get(parser.Props.SCAN_FILTER, {})

        condition_map = parser.Parser.parse_attribute_conditions(
            scan_filter
        )

        segment = body.get(parser.Props.SEGMENT, 0)
        total_segments = body.get(parser.Props.TOTAL_SEGMENTS, 1)

        assert segment < total_segments

        result = storage.scan(
            req.context, table_name, condition_map,
            attributes_to_get=attrs_to_get, limit=limit,
            exclusive_start_key=exclusive_start_key)

        response = {
            parser.Props.COUNT: result.count,
            parser.Props.SCANNED_COUNT: result.scanned_count
        }

        if not select_type.is_count:
            response[parser.Props.ITEMS] = [
                parser.Parser.format_item_attributes(row)
                for row in result.items]

        if result.last_evaluated_key:
            response[parser.Props.LAST_EVALUATED_KEY] = (
                parser.Parser.format_item_attributes(
                    result.last_evaluated_key
                )
            )

        return response
