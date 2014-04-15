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
from magnetodb.storage import models

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils

LOG = logging.getLogger(__name__)


class QueryController(object):
    schema = {
        "required": [parser.Props.KEY_CONDITIONS],
        "properties": {
            parser.Props.ATTRIBUTES_TO_GET: {
                "type": "array",
                "items": {
                    "type": "string",
                    "pattern": parser.ATTRIBUTE_NAME_PATTERN
                }
            },

            parser.Props.CONSISTENT_READ: {
                "type": "boolean"
            },

            parser.Props.EXCLUSIVE_START_KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },

            parser.Props.INDEX_NAME: {
                "type": "string",
                "pattern": parser.INDEX_NAME_PATTERN
            },

            parser.Props.KEY_CONDITIONS: {
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
                                parser.Types.QUERY_OPERATOR
                            )
                        }
                    }
                }
            },

            parser.Props.LIMIT: {
                "type": "number"
            },

            parser.Props.SCAN_INDEX_FORWARD: {
                "type": "boolean"
            },

            parser.Props.SELECT: parser.Types.SELECT
        }
    }

    def query(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)
        req.context.tenant = project_id

        # parse select_type
        attributes_to_get = body.get(parser.Props.ATTRIBUTES_TO_GET)

        if attributes_to_get is not None:
            attributes_to_get = frozenset(attributes_to_get)

        select = body.get(parser.Props.SELECT)

        index_name = body.get(parser.Props.INDEX_NAME)

        select_type = parser.Parser.parse_select_type(select,
                                                      attributes_to_get,
                                                      index_name)

        # parse exclusive_start_key_attributes
        exclusive_start_key_attributes = body.get(
            parser.Props.EXCLUSIVE_START_KEY)

        if exclusive_start_key_attributes is not None:
            exclusive_start_key_attributes = (
                parser.Parser.parse_item_attributes(
                    exclusive_start_key_attributes
                )
            )

        # parse indexed_condition_map
        indexed_condition_map = parser.Parser.parse_attribute_conditions(
            body.get(parser.Props.KEY_CONDITIONS))

        # TODO(dukhlov):
        # it would be nice to validate given table_name, key_attributes and
        # attributes_to_get to schema expectation

        consistent_read = body.get(
            parser.Props.CONSISTENT_READ, False)

        limit = body.get(parser.Props.LIMIT)

        order_asc = body.get(parser.Props.SCAN_INDEX_FORWARD)

        if order_asc is None:
            order_type = None
        elif order_asc:
            order_type = models.ORDER_TYPE_ASC
        else:
            order_type = models.ORDER_TYPE_DESC

        # select item
        result = storage.select_item(
            req.context, table_name, indexed_condition_map,
            select_type=select_type, index_name=index_name, limit=limit,
            consistent=consistent_read, order_type=order_type,
            exclusive_start_key=exclusive_start_key_attributes
        )

        # format response
        if select_type.type == models.SelectType.SELECT_TYPE_COUNT:
            response = {
                parser.Props.COUNT: result.count
            }
        else:
            response = {
                parser.Props.COUNT: result.count,
                parser.Props.ITEMS: [
                    parser.Parser.format_item_attributes(row)
                    for row in result.items
                ]
            }

        if limit == result.count:
            response[parser.Props.LAST_EVALUATED_KEY] = (
                parser.Parser.format_item_attributes(
                    result.last_evaluated_key)
            )

        return response
