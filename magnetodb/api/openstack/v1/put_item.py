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

from magnetodb import storage
from magnetodb.api import validation
from magnetodb.storage import models

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils

from magnetodb.openstack.common.gettextutils import _
from magnetodb.storage.models import InsertReturnValuesType


class PutItemController(object):
    """ Creates a new item, or replaces an old item. """

    def process_request(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id

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

        return_values = InsertReturnValuesType(return_values_json)

        # parse return_values param
        time_to_live = body.pop(
            parser.Props.TIME_TO_LIVE, None
        )

        if time_to_live is not None:
            validation.validate_integer(time_to_live,
                                        parser.Props.TIME_TO_LIVE)

        validation.validate_unexpected_props(body, "body")

        if (return_values.type ==
                InsertReturnValuesType.RETURN_VALUES_TYPE_ALL_OLD):
            m = _("return_values %s is not supported for now")
            raise NotImplementedError(
                m % InsertReturnValuesType.RETURN_VALUES_TYPE_ALL_OLD
            )

        storage.put_item(
            req.context,
            models.PutItemRequest(table_name, item_attributes),
            if_not_exist=False,
            expected_condition_map=expected_item_conditions)

        # format response
        response = {}

        return response
