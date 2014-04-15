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

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb import storage
from magnetodb.storage import models


class GetItemController(object):
    schema = {
        "required": [parser.Props.KEY],
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
            parser.Props.KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },
        }
    }

    def process_request(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)
        req.context.tenant = project_id

        # get attributes_to_get
        attributes_to_get = body.get(parser.Props.ATTRIBUTES_TO_GET)

        select_type = (
            models.SelectType.all()
            if attributes_to_get is None else
            models.SelectType.specified_attributes(attributes_to_get)
        )

        # parse key_attributes
        key_attributes = parser.Parser.parse_item_attributes(
            body[parser.Props.KEY]
        )

        # parse consistent_read
        consistent_read = body.get(
            parser.Props.CONSISTENT_READ, False
        )

        # format conditions to get item
        indexed_condition_map = {
            name: [models.IndexedCondition.eq(value)]
            for name, value in key_attributes.iteritems()
        }

        # get item
        result = storage.select_item(
            req.context, table_name, indexed_condition_map,
            select_type=select_type, limit=2, consistent=consistent_read)

        # format response
        if result.count == 0:
            return {}

        response = {
            parser.Props.ITEM: parser.Parser.format_item_attributes(
                result.items[0])
        }
        return response
