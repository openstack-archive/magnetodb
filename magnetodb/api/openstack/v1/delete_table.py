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
from magnetodb.openstack.common.log import logging

from magnetodb.api.openstack.v1 import parser
from magnetodb.api.openstack.v1 import utils


LOG = logging.getLogger(__name__)


class DeleteTableController(object):
    def delete_table(self, req, project_id, table_name):
        utils.check_project_id(req.context, project_id)
        req.context.tenant = project_id

        table_schema = storage.delete_table(req.context, table_name)

        url = req.path_url
        bookmark = req.path_url

        return {
            parser.Props.TABLE_DESCRIPTION: {
                parser.Props.ATTRIBUTE_DEFINITIONS: (
                    parser.Parser.format_attribute_definitions(
                        table_schema.schema.attribute_type_map
                    )
                ),
                parser.Props.CREATION_DATE_TIME: 0,
                parser.Props.ITEM_COUNT: 0,
                parser.Props.KEY_SCHEMA: (
                    parser.Parser.format_key_schema(
                        table_schema.schema.key_attributes
                    )
                ),
                parser.Props.LOCAL_SECONDARY_INDEXES: (
                    parser.Parser.format_local_secondary_indexes(
                        table_schema.schema.key_attributes[0],
                        table_schema.schema.index_def_map
                    )
                ),
                parser.Props.TABLE_NAME: table_name,

                parser.Props.TABLE_STATUS: (
                    parser.Parser.format_table_status(table_schema.status)),
                parser.Props.TABLE_SIZE_BYTES: 0,

                parser.Props.LINKS: [
                    {
                        parser.Props.HREF: url,
                        parser.Props.REL: parser.Values.SELF
                    },
                    {
                        parser.Props.HREF: bookmark,
                        parser.Props.REL: parser.Values.BOOKMARK
                    }
                ]
            }
        }
