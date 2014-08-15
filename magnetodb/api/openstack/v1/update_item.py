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

import jsonschema

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb import storage
from magnetodb.common import exception


class UpdateItemController(object):
    """
    Edits(or inserts if item does not already exist) an item's attributes.
    """

    schema = {
        "required": [parser.Props.KEY],
        "properties": {
            parser.Props.EXPECTED: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "type": "object",
                        "properties": {
                            parser.Props.EXISTS: {
                                "type": "boolean",
                            },
                            parser.Props.VALUE: (
                                parser.Types.TYPED_ATTRIBUTE_VALUE
                            )

                        }
                    }
                }
            },

            parser.Props.ATTRIBUTE_UPDATES: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: {
                        "type": "object",
                        "required": [parser.Props.ACTION],
                        "properties": {
                            parser.Props.ACTION: parser.Types.ACTION_TYPE,
                            parser.Props.VALUE: (
                                parser.Types.TYPED_ATTRIBUTE_VALUE
                            )
                        }
                    }
                }
            },

            parser.Props.KEY: parser.Types.ITEM,

            parser.Props.RETURN_VALUES: {
                "type": "string",
                "enum": [parser.Values.RETURN_VALUES_NONE,
                         parser.Values.RETURN_VALUES_ALL_OLD]
            },
        }
    }

    def process_request(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)
        req.context.tenant = project_id

        # parse expected item conditions
        expected_item_conditions = (
            parser.Parser.parse_expected_attribute_conditions(
                body.get(parser.Props.EXPECTED, {})))

        # parse attribute updates
        attribute_updates = parser.Parser.parse_attribute_updates(
            body.get(parser.Props.ATTRIBUTE_UPDATES, {}))

        # parse key
        key_attribute_map = parser.Parser.parse_item_attributes(
            body[parser.Props.KEY])

        # parse return_values param
        return_values = body.get(parser.Props.RETURN_VALUES,
                                 parser.Values.RETURN_VALUES_NONE)

        # update item
        result, old_item = storage.update_item(
            req.context,
            table_name,
            key_attribute_map=key_attribute_map,
            attribute_action_map=attribute_updates,
            expected_condition_map=expected_item_conditions)

        if not result:
            raise exception.BackendInteractionException()

        # format response
        response = {}

        if return_values != parser.Values.RETURN_VALUES_NONE:
            response[parser.Props.ATTRIBUTES] = (
                parser.Parser.format_item_attributes(old_item)
            )

        return response
