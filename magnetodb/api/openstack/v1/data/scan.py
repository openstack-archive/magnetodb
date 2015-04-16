# Copyright 2014 Mirantis Inc.
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
from magnetodb.openstack.common import log as logging
from magnetodb import storage
from magnetodb.storage import models

LOG = logging.getLogger(__name__)


@api.enforce_policy("mdb:scan")
@probe.Probe(__name__)
@request_context_decorator.request_type("scan")
def scan(req, project_id, table_name):
    """The Scan operation returns one or more items
    and item attributes by accessing every item in the table.
    """

    with probe.Probe(__name__ + '.validation'):
        body = req.json_body
        validation.validate_object(body, "body")

        # get attributes_to_get
        attributes_to_get = body.pop(parser.Props.ATTRIBUTES_TO_GET, None)
        if attributes_to_get:
            validation.validate_list(attributes_to_get,
                                     parser.Props.ATTRIBUTES_TO_GET)
            for attr_name in attributes_to_get:
                validation.validate_attr_name(attr_name)

        select = body.pop(parser.Props.SELECT, None)

        if select is None:
            if attributes_to_get:
                select = models.SelectType.SELECT_TYPE_SPECIFIC
            else:
                select = models.SelectType.SELECT_TYPE_ALL
        else:
            validation.validate_string(select, parser.Props.SELECT)
        select_type = models.SelectType(select, attributes_to_get)

        limit = body.pop(parser.Props.LIMIT, None)
        if limit is not None:
            limit = validation.validate_integer(limit, parser.Props.LIMIT,
                                                min_val=0)

        # parse exclusive_start_key_attributes
        exclusive_start_key_attributes_json = body.pop(
            parser.Props.EXCLUSIVE_START_KEY, None)
        if exclusive_start_key_attributes_json is not None:
            validation.validate_object(exclusive_start_key_attributes_json,
                                       parser.Props.EXCLUSIVE_START_KEY)
            exclusive_start_key_attributes = (
                parser.Parser.parse_item_attributes(
                    exclusive_start_key_attributes_json
                )
            )
        else:
            exclusive_start_key_attributes = None

        scan_filter_json = body.pop(parser.Props.SCAN_FILTER, None)
        if scan_filter_json:
            validation.validate_object(scan_filter_json,
                                       parser.Props.SCAN_FILTER)

            condition_map = parser.Parser.parse_attribute_conditions(
                scan_filter_json, condition_class=models.ScanCondition
            )
        else:
            condition_map = None

        total_segments = body.pop(parser.Props.TOTAL_SEGMENTS, 1)
        total_segments = validation.validate_integer(
            total_segments, parser.Props.TOTAL_SEGMENTS, min_val=1,
            max_val=4096
        )

        segment = body.pop(parser.Props.SEGMENT, 0)
        segment = validation.validate_integer(
            segment, parser.Props.SEGMENT, min_val=0,
            max_val=total_segments
        )

        validation.validate_unexpected_props(body, "body")

    result = storage.scan(
        project_id, table_name, condition_map,
        attributes_to_get=attributes_to_get, limit=limit,
        exclusive_start_key=exclusive_start_key_attributes)

    response = {
        parser.Props.COUNT: result.count,
        parser.Props.SCANNED_COUNT: result.scanned_count
    }

    if not select_type.is_count:
        response[parser.Props.ITEMS] = [
            parser.Parser.format_item_attributes(row)
            for row in result.items]

    if result.last_evaluated_key:
        response[parser.Props.LAST_EVALUATED_KEY] = (
            parser.Parser.format_item_attributes(
                result.last_evaluated_key
            )
        )

    return response
