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

import collections
import jsonschema

from magnetodb import storage

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils


class BatchGetItemController(object):
    REQUEST_GET_SCEMA = {
        "type": "object",
        "required": [parser.Props.KEYS],
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
            parser.Props.KEYS: {
                "type": "array",
                "items": {
                    "type": "object",
                    "minProperties": 1,
                    "maxProperties": 2,
                    "patternProperties": {
                        parser.ATTRIBUTE_NAME_PATTERN:
                            parser.Types.SINGLE_ITEM_VALUE
                    }
                }
            }
        }
    }

    schema = {
        "required": [parser.Props.REQUEST_ITEMS],
        "properties": {
            parser.Props.REQUEST_ITEMS: {
                "type": "object",
                "patternProperties": {
                    parser.TABLE_NAME_PATTERN: {
                        "type": "object",
                        "items": REQUEST_GET_SCEMA
                    }
                }
            }
        }
    }

    def process_request(self, req, body, project_id):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)

        # parse request_items
        request_items = parser.Parser.parse_batch_get_request_items(
            body[parser.Props.REQUEST_ITEMS])

        req.context.tenant = project_id

        request_list = collections.deque()
        for rq_item in request_items:
            request_list.append(rq_item)

        result, unprocessed_items = storage.execute_get_batch(
            req.context, request_list)

        responses = {}
        for tname, res in result:
            if not res.items:
                continue
            if tname not in responses:
                table_items = []
                responses[tname] = table_items
            item = parser.Parser.format_item_attributes(res.items[0])
            table_items.append(item)

        return {
            'responses': responses,
            'unprocessed_items': parser.Parser.format_batch_get_unprocessed(
                unprocessed_items, body[parser.Props.REQUEST_ITEMS])
        }
