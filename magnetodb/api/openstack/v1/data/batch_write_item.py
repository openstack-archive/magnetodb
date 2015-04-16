# Copyright 2014 Symantec Corporation
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

from magnetodb import api
from magnetodb.api.openstack.v1 import parser
from magnetodb.api import validation
from magnetodb.common import probe
from magnetodb.common.utils import request_context_decorator
from magnetodb import storage


@api.enforce_policy("mdb:batch_write_item")
@probe.Probe(__name__)
@request_context_decorator.request_type("batch_write")
def batch_write_item(req, project_id):
    """The BatchWriteItem operation puts or deletes
    multiple items in one or more tables.
    """

    with probe.Probe(__name__ + '.validation'):
        body = req.json_body
        validation.validate_object(body, "body")

        request_items_json = body.pop(parser.Props.REQUEST_ITEMS, None)
        validation.validate_object(request_items_json,
                                   parser.Props.REQUEST_ITEMS)

        validation.validate_unexpected_props(body, "body")

        # parse request_items
        request_map = parser.Parser.parse_batch_write_request_items(
            request_items_json
        )

    unprocessed_items = storage.execute_write_batch(project_id, request_map)

    return {
        'unprocessed_items': parser.Parser.format_request_items(
            unprocessed_items)}
