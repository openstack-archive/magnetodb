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
import logging

from magnetodb.common.exception import ResourceInUseException
from magnetodb.common.exception import TableAlreadyExistsException
from magnetodb.common.exception import TableNotExistsException

from magnetodb import notifier

from magnetodb.storage import models
from magnetodb.storage.manager.simple_impl import SimpleStorageManager
from magnetodb.storage.table_info_repo import TableInfo

LOG = logging.getLogger(__name__)


class BaseAsyncStorageManager(SimpleStorageManager):
    def __init__(self, storage_driver,
                 table_info_repo,
                 concurrent_tasks=1000, batch_chunk_size=25):
        SimpleStorageManager.__init__(self, storage_driver, table_info_repo,
                                      concurrent_tasks, batch_chunk_size)

    def _async_create(self, context, table_info):
        raise NotImplementedError()

    def create_table(self, context, table_name, table_schema):
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_START,
                        table_schema)

        table_info = TableInfo(table_name, table_schema,
                               models.TableMeta.TABLE_STATUS_CREATING)
        try:
            self._table_info_repo.save(context, table_info)
        except TableAlreadyExistsException as e:
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_ERROR,
                            e.message, priority=notifier.PRIORITY_ERROR)
            raise

        self._async_create(context, table_info)

        return models.TableMeta(table_info.schema, table_info.status)

    def _async_delete(self, context, table_info):
        raise NotImplementedError()

    def delete_table(self, context, table_name):
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_START,
                        table_name)
        try:
            table_info = self._table_info_repo.get(context,
                                                   table_name,
                                                   ['status'])
        except TableNotExistsException as e:
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_ERROR,
                            e.message, priority=notifier.PRIORITY_ERROR)
            raise

        if table_info.status == models.TableMeta.TABLE_STATUS_DELETING:
            # table is already being deleted, just return immediately
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_END,
                            table_name)
            return models.TableMeta(table_info.schema, table_info.status)
        elif table_info.status != models.TableMeta.TABLE_STATUS_ACTIVE:
            e = ResourceInUseException()
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_ERROR,
                            table_name + ' ' + e.message,
                            priority=notifier.PRIORITY_ERROR)
            raise e

        table_info.status = models.TableMeta.TABLE_STATUS_DELETING

        self._table_info_repo.update(context, table_info, ["status"])

        self._async_delete(context, table_info)

        return models.TableMeta(table_info.schema, table_info.status)
