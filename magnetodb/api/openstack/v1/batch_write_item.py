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


class BatchWriteItemController(object):
    REQUEST_DELETE_SCEMA = {
        "type": "object",
        "required": [parser.Props.REQUEST_DELETE],
        "properties": {
            parser.Props.REQUEST_DELETE: {
                "type": "object",
                "required": [parser.Props.KEY],
                "properties": {
                    parser.Props.KEY: {
                        "type": "object",
                        "patternProperties": {
                            parser.ATTRIBUTE_NAME_PATTERN:
                                parser.Types.ITEM_VALUE
                        }
                    }
                }
            }
        }
    }

    REQUEST_PUT_SCEMA = {
        "type": "object",
        "required": [parser.Props.REQUEST_PUT],
        "properties": {
            parser.Props.REQUEST_PUT: {
                "type": "object",
                "required": [parser.Props.ITEM],
                "properties": {
                    parser.Props.ITEM: {
                        "type": "object",
                        "patternProperties": {
                            parser.ATTRIBUTE_NAME_PATTERN:
                                parser.Types.ITEM_VALUE
                        }
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
                        "type": "array",
                        "items": {
                            "oneOf": [
                                REQUEST_DELETE_SCEMA,
                                REQUEST_PUT_SCEMA
                            ]
                        }
                    }
                }
            },
        }
    }

    def process_request(self, req, body, project_id):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)

        # parse request_items
        request_items = parser.Parser.parse_request_items(
            body[parser.Props.REQUEST_ITEMS])

        req.context.tenant = project_id

        request_list = collections.deque()
        for rq_item in request_items:
            request_list.append(rq_item)

        unprocessed_items = storage.execute_write_batch(
            req.context, request_list)

        return {
            'unprocessed_items': parser.Parser.format_request_items(
                unprocessed_items)}
