# Copyright 2014 Symantec Inc.
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


class UpdateItemController(object):
    SCHEMA = {
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
                            parser.Props.VALUE: parser.Types.ITEM_VALUE
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
                            parser.Props.VALUE: parser.Types.ITEM_VALUE
                        }
                    }
                }
            },

            parser.Props.KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },

            parser.Props.RETURN_VALUES: {
                "type": "string",
                "enum": [parser.Values.RETURN_VALUES_NONE,
                         parser.Values.RETURN_VALUES_ALL_OLD,
                         parser.Values.RETURN_VALUES_ALL_NEW,
                         parser.Values.RETURN_VALUES_UPDATED_OLD,
                         parser.Values.RETURN_VALUES_UPDATED_NEW]
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

        #parse attribute updates
        attribute_updates = parser.Parser.parse_attribute_updates(
            body.get(parser.Props.ATTRIBUTE_UPDATES, {}))

        # parse key
        key_attributes = parser.Parser.parse_item_attributes(
            body[parser.Props.KEY])
