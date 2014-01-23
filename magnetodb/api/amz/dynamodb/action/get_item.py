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

from magnetodb.api.amz.dynamodb.action import DynamoDBAction
from magnetodb.api.amz.dynamodb import parser

from magnetodb import storage
from magnetodb.common import exception
from magnetodb.storage import models


class GetItemDynamoDBAction(DynamoDBAction):
    schema = {
        "required": [parser.Props.KEY,
                     parser.Props.TABLE_NAME],
        "properties": {
            parser.Props.ATTRIBUTES_TO_GET: {
                "type": "array",
                "items": {
                    "type": "string",
                    "pattern": parser.ATTRIBUTE_NAME_PATTERN
                }
            },
            parser.Props.CONSISTENT_READ: {
                "type": "boolean"
            },
            parser.Props.KEY: {
                "type": "object",
                "patternProperties": {
                    parser.ATTRIBUTE_NAME_PATTERN: parser.Types.ITEM_VALUE
                }
            },
            parser.Props.RETURN_CONSUMED_CAPACITY: (
                parser.Types.RETURN_CONSUMED_CAPACITY
            ),
            parser.Props.TABLE_NAME: parser.Types.TABLE_NAME
        }
    }

    def __call__(self):
        try:
            table_name = self.action_params.get(parser.Props.TABLE_NAME, None)

            # get attributes_to_get
            attributes_to_get = self.action_params.get(
                parser.Props.ATTRIBUTES_TO_GET, None
            )

            select_type = (
                models.SelectType.all()
                if attributes_to_get is None else
                models.AttributeToGet.specified(attributes_to_get)
            )

            # parse key_attributes
            key_attributes = parser.Parser.parse_item_attributes(
                self.action_params[parser.Props.KEY]
            )

            # TODO(dukhlov):
            # it would be nice to validate given table_name, key_attributes and
            # attributes_to_get  to schema expectation

            consistent_read = self.action_params.get(
                parser.Props.CONSISTENT_READ, False
            )

            return_consumed_capacity = self.action_params.get(
                parser.Props.RETURN_CONSUMED_CAPACITY,
                parser.Values.RETURN_CONSUMED_CAPACITY_NONE
            )

            # format conditions to get item
            indexed_condition_map = {
                name: models.IndexedCondition.eq(value)
                for name, value in key_attributes.iteritems()
            }
        except Exception:
            raise exception.ValidationException()

        try:
            # get item
            result = storage.select_item(
                self.context, table_name, indexed_condition_map,
                select_type=select_type, limit=2, consistent=consistent_read)

            # format response
            if result.count == 0:
                return {}

            assert result.count == 1

            response = {
                parser.Props.ITEM: parser.Parser.format_item_attributes(
                    result.items[0])
            }

            if (return_consumed_capacity !=
                    parser.Values.RETURN_CONSUMED_CAPACITY_NONE):
                response[parser.Props.CONSUMED_CAPACITY] = (
                    parser.Parser.format_consumed_capacity(
                        return_consumed_capacity, None
                    )
                )

            return response
        except exception.AWSErrorResponseException as e:
            raise e
        except Exception:
            raise exception.AWSErrorResponseException()
