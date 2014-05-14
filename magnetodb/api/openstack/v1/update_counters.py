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
#from magnetodb import storage


class UpdateCountersController(object):
    schema = {
        "required": [parser.Props.KEY],
        "properties": {
            parser.Props.KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },
            parser.Props.COUNTERS: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.COUNTER_VALUE
                }
            },
        }
    }

    def process_request(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        jsonschema.validate(body, self.schema)
        req.context.tenant = project_id

        # parse key_attributes
#        key_attributes = parser.Parser.parse_item_attributes(
#            body[parser.Props.KEY])

        #parse counters
        counters = parser.Parser.parse_counters(
            body.get(parser.Props.COUNTERS))

        # update counters
        if counters:
            # for name, value in counters.iteritems():
            #    storage.update_counter(req.context, table_name,
            #       key_attributes, name, value)
            pass

        # TODO(achudnovets): what we need to return?
        return {}
