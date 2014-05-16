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
import logging

from magnetodb.common.exception import ResourceInUseException
from magnetodb.storage import models
from magnetodb.storage.manager.simple_impl import SimpleStorageManager
from magnetodb.storage.table_info_repo import TableInfo

LOG = logging.getLogger(__name__)


class AsyncSimpleStorageManager(SimpleStorageManager):
    def create_table(self, context, table_name, table_schema):
        table_info = TableInfo(table_name, table_schema,
                               models.TableMeta.TABLE_STATUS_CREATING)
        self._table_info_repo.save(context, table_info)

        future = self._execute_async(self._storage_driver.create_table,
                                     context=context,
                                     table_name=table_name)

        def callback(future):
            if not future.exception():
                latest_table_info = (
                    TableInfo(table_name,
                              table_schema,
                              models.TableMeta.TABLE_STATUS_ACTIVE))
                self._table_info_repo.update(
                    context, latest_table_info, ["status"]
                )

        future.add_done_callback(callback)

        return models.TableMeta(table_info.schema, table_info.status)

    def delete_table(self, context, table_name):
        table_info = self._table_info_repo.get(context, table_name, ['status'])

        # table has to be in active status to be deleted
        if table_info.status == models.TableMeta.TABLE_STATUS_CREATING:
            raise ResourceInUseException()
        elif table_info.status == models.TableMeta.TABLE_STATUS_DELETING:
            # table is already being deleted, just return immediately
            return models.TableMeta(table_info.schema, table_info.status)

        table_info.status = models.TableMeta.TABLE_STATUS_DELETING

        self._table_info_repo.update(context, table_info, ["status"])

        future = self._execute_async(self._storage_driver.delete_table,
                                     context=context,
                                     table_name=table_name)

        def callback(future):
            if not future.exception():
                self._table_info_repo.delete(
                    context, table_name
                )

        future.add_done_callback(callback)
        return models.TableMeta(table_info.schema, table_info.status)
