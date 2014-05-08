# Copyright 2013 Mirantis Inc.
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
from magnetodb.storage import models

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils


class PutItemController(object):
    schema = {
        "required": [parser.Props.ITEM],
        "properties": {
            parser.Props.EXPECTED: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "oneOf": [
                            {
                                "type": "object",
                                "required": [parser.Props.EXISTS],
                                "properties": {
                                    parser.Props.EXISTS: {
                                        "type": "boolean",
                                    },
                                }
                            },
                            {
                                "type": "object",
                                "required": [parser.Props.VALUE],
                                "properties": {
                                    parser.Props.VALUE:
                                        parser.Types.ITEM_VALUE,
                                }
                            },
                        ]
                    }
                }
            },

            parser.Props.ITEM: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },

            parser.Props.RETURN_VALUES: {
                "type": "string",
                "enum": [parser.Values.RETURN_VALUES_NONE,
                         parser.Values.RETURN_VALUES_ALL_OLD]
            }
        }
    }

    def process_request(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)

        # parse expected item conditions
        expected_item_conditions = (
            parser.Parser.parse_expected_attribute_conditions(
                body.get(parser.Props.EXPECTED, {})
            )
        )

        # parse item
        item_attributes = parser.Parser.parse_item_attributes(
            body[parser.Props.ITEM]
        )

        # parse return_values param
        return_values = body.get(
            parser.Props.RETURN_VALUES, parser.Values.RETURN_VALUES_NONE
        )

        # put item
        req.context.tenant = project_id
        storage.put_item(
            req.context,
            models.PutItemRequest(table_name, item_attributes),
            if_not_exist=False,
            expected_condition_map=expected_item_conditions)

        # format response
        response = {}

        if return_values != parser.Values.RETURN_VALUES_NONE:
            response[parser.Props.ATTRIBUTES] = (
                parser.Parser.format_item_attributes(item_attributes)
            )
        return response
