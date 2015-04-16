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
from magnetodb.common import probe
from magnetodb.common.utils import request_context_decorator
from magnetodb import storage
from magnetodb.storage import models


@api.enforce_policy("mdb:delete_item")
@probe.Probe(__name__)
@request_context_decorator.request_type("delete_item")
def delete_item(req, project_id, table_name):
    """Deletes a single item in a table by primary key. """

    with probe.Probe(__name__ + '.jsonschema.validate'):
        body = req.json_body
        validation.validate_object(body, "body")

        # parse expected item conditions
        expected_item_conditions_json = body.pop(parser.Props.EXPECTED,
                                                 None)
        if expected_item_conditions_json:
            validation.validate_object(expected_item_conditions_json,
                                       parser.Props.EXPECTED)
            expected_item_conditions = (
                parser.Parser.parse_expected_attribute_conditions(
                    expected_item_conditions_json
                )
            )
        else:
            expected_item_conditions = None

        # parse key_attributes
        key_attributes_json = body.pop(parser.Props.KEY, None)
        validation.validate_object(key_attributes_json, parser.Props.KEY)

        key_attributes = parser.Parser.parse_item_attributes(
            key_attributes_json
        )

        # parse return_values param
        return_values_json = body.pop(
            parser.Props.RETURN_VALUES, parser.Values.RETURN_VALUES_NONE
        )

        validation.validate_string(return_values_json,
                                   parser.Props.RETURN_VALUES)

        return_values = models.DeleteReturnValuesType(return_values_json)

        validation.validate_unexpected_props(body, "body")

    # delete item
    storage.delete_item(project_id, table_name, key_attributes,
                        expected_condition_map=expected_item_conditions)

    # format response
    response = {}

    if return_values.type != parser.Values.RETURN_VALUES_NONE:
        # TODO(cwang):
        # It is needed to return all deleted item attributes
        #
        response[parser.Props.ATTRIBUTES] = (
            parser.Parser.format_item_attributes(key_attributes)
        )

    return response
