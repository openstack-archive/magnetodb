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

from magnetodb.api.amz.dynamodb import action
from magnetodb.api.amz import parser
from magnetodb.api.amz import exception as azm_exception
from magnetodb import storage


class ListTablesDynamoDBAction(action.DynamoDBAction):
    schema = {
        "type": "object",
        "properties": {
            parser.Props.EXCLUSIVE_START_TABLE_NAME:  parser.Types.TABLE_NAME,
            parser.Props.LIMIT: {
                "type": "integer",
                "minimum": 0
            }
        }
    }

    def __call__(self):
        exclusive_start_table_name = (
            self.action_params.get(parser.Props.EXCLUSIVE_START_TABLE_NAME,
                                   None)
        )

        limit = self.action_params.get(parser.Props.LIMIT, None)

        try:
            table_names = (
                storage.list_tables(
                    self.tenant,
                    exclusive_start_table_name=exclusive_start_table_name,
                    limit=limit)
            )

            res = {parser.Props.TABLE_NAMES: table_names}

            if table_names and limit == len(table_names):
                res[parser.Props.LAST_EVALUATED_TABLE_NAME] = table_names[-1]

            return res
        except Exception:
            raise azm_exception.AWSErrorResponseException()
