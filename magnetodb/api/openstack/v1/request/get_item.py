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

from magnetodb.api import validation

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils
from magnetodb.common import probe
from magnetodb import storage
from magnetodb.storage import models


class GetItemController(object):
    """The Getitem operation returns an item with the given primary key. """

    @probe.Probe(__name__)
    def process_request(self, req, body, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id

        with probe.Probe(__name__ + '.validate'):
            validation.validate_object(body, "body")

            # get attributes_to_get
            attributes_to_get = body.pop(parser.Props.ATTRIBUTES_TO_GET, None)
            if attributes_to_get:
                attributes_to_get = validation.validate_set(
                    attributes_to_get, parser.Props.ATTRIBUTES_TO_GET
                )
                for attr_name in attributes_to_get:
                    validation.validate_attr_name(attr_name)
                select_type = models.SelectType.specific_attributes(
                    attributes_to_get
                )
            else:
                select_type = models.SelectType.all()

            key = body.pop(parser.Props.KEY, None)
            validation.validate_object(key, parser.Props.KEY)

            # parse consistent_read
            consistent_read = body.pop(parser.Props.CONSISTENT_READ, False)
            validation.validate_boolean(consistent_read,
                                        parser.Props.CONSISTENT_READ)

            validation.validate_unexpected_props(body, "body")

            # parse key_attributes
            key_attributes = parser.Parser.parse_item_attributes(key)

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
