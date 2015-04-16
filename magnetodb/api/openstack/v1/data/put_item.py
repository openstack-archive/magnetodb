# Copyright 2013 Mirantis Inc.
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


@api.enforce_policy("mdb:put_item")
@probe.Probe(__name__)
@request_context_decorator.request_type("put_item")
def put_item(req, project_id, table_name):
    """Creates a new item, or replaces an old item. """

    with probe.Probe(__name__ + '.validation'):
        body = req.json_body
        validation.validate_object(body, "body")

        expected = body.pop(parser.Props.EXPECTED, {})
        validation.validate_object(expected, parser.Props.EXPECTED)
        # parse expected item conditions
        expected_item_conditions = (
            parser.Parser.parse_expected_attribute_conditions(expected)
        )

        item = body.pop(parser.Props.ITEM, None)
        validation.validate_object(item, parser.Props.ITEM)
        # parse item
        item_attributes = parser.Parser.parse_item_attributes(item)

        # parse return_values param
        return_values_json = body.pop(
            parser.Props.RETURN_VALUES, parser.Values.RETURN_VALUES_NONE
        )

        validation.validate_string(return_values_json,
                                   parser.Props.RETURN_VALUES)

        return_values = models.InsertReturnValuesType(return_values_json)

        # parse return_values param
        time_to_live = body.pop(
            parser.Props.TIME_TO_LIVE, None
        )

        if time_to_live is not None:
            time_to_live = validation.validate_integer(
                time_to_live, parser.Props.TIME_TO_LIVE, min_val=0
            )

        validation.validate_unexpected_props(body, "body")

    # put item
    result, old_item = storage.put_item(
        project_id, table_name, item_attributes,
        return_values=return_values,
        if_not_exist=False,
        expected_condition_map=expected_item_conditions,
    )

    response = {}

    if old_item:
        response[parser.Props.ATTRIBUTES] = (
            parser.Parser.format_item_attributes(old_item)
        )

    return response
