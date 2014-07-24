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

import logging

from threading import BoundedSemaphore
from threading import Event

import weakref

from concurrent.futures import ThreadPoolExecutor

from magnetodb.common.exception import TableAlreadyExistsException
from magnetodb.common.exception import BackendInteractionException

from magnetodb import notifier

from magnetodb.storage import models
from magnetodb.storage.manager import StorageManager
from magnetodb.storage.table_info_repo import TableInfo

LOG = logging.getLogger(__name__)


class SimpleStorageManager(StorageManager):

    def __init__(self, storage_driver, table_info_repo, concurrent_tasks=1000):
        self._storage_driver = storage_driver
        self._table_info_repo = table_info_repo
        self.__task_executor = ThreadPoolExecutor(concurrent_tasks)
        self.__task_semaphore = BoundedSemaphore(concurrent_tasks)

    def __del__(self):
        self.__task_executor.shutdown()

    def create_table(self, context, table_name, table_schema):
        table_info = TableInfo(table_name, table_schema,
                               models.TableMeta.TABLE_STATUS_CREATING)
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_START,
                        table_schema)

        try:
            self._table_info_repo.save(context, table_info)
        except TableAlreadyExistsException as e:
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_ERROR,
                            e.message, priority=notifier.PRIORITY_ERROR)
            raise

        try:
            self._storage_driver.create_table(context, table_name)
            table_info.status = models.TableMeta.TABLE_STATUS_ACTIVE
            self._table_info_repo.update(
                context, table_info, ["status"]
            )
        except BackendInteractionException as ex:
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_ERROR,
                            ex.message, priority=notifier.PRIORITY_ERROR)
            raise

        notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_END,
                        table_schema)

        return models.TableMeta(table_info.schema, table_info.status)

    def delete_table(self, context, table_name):
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_START,
                        table_name)

        table_info = self._table_info_repo.get(context, table_name)

        table_info.status = models.TableMeta.TABLE_STATUS_DELETING

        self._table_info_repo.update(context, table_info, ["status"])

        self._storage_driver.delete_table(context, table_name)

        self._table_info_repo.delete(context, table_name)

        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_END,
                        table_name)

        return models.TableMeta(table_info.schema, table_info.status)

    def describe_table(self, context, table_name):
        table_info = self._table_info_repo.get(context,
                                               table_name,
                                               ['status'])
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DESCRIBE,
                        table_name, priority=notifier.PRIORITY_DEBUG)

        return models.TableMeta(table_info.schema, table_info.status)

    def list_tables(self, context, exclusive_start_table_name=None,
                    limit=None):
        tnames = self._table_info_repo.get_tenant_table_names(
            context, exclusive_start_table_name, limit
        )
        notifier.notify(
            context, notifier.EVENT_TYPE_TABLE_LIST,
            dict(
                exclusive_start_table_name=exclusive_start_table_name,
                limit=limit
            ),
            priority=notifier.PRIORITY_DEBUG
        )

        return tnames

    def _execute_async(self, func, *args, **kwargs):
        weak_self = weakref.proxy(self)

        def callback(future):
            weak_self.__task_semaphore.release()

        self.__task_semaphore.acquire()
        future = self.__task_executor.submit(func, *args, **kwargs)
        future.add_done_callback(callback)
        return future

    def put_item(self, context, put_request, if_not_exist=False,
                 expected_condition_map=None):
        with self.__task_semaphore:
            result = self._storage_driver.put_item(context, put_request,
                                                   if_not_exist,
                                                   expected_condition_map)
            notifier.notify(
                context, notifier.EVENT_TYPE_DATA_PUTITEM,
                dict(
                    put_request=put_request,
                    if_not_exist=if_not_exist,
                    expected_condition_map=expected_condition_map
                ),
                priority=notifier.PRIORITY_DEBUG
            )

            return result

    def put_item_async(self, context, put_request, if_not_exist=False,
                       expected_condition_map=None):
        notifier.notify(
            context, notifier.EVENT_TYPE_DATA_PUTITEM_START,
            dict(
                put_request=put_request,
                if_not_exist=if_not_exist,
                expected_condition_map=expected_condition_map
            )
        )

        put_future = self._execute_async(
            self._storage_driver.put_item,
            context, put_request, if_not_exist, expected_condition_map
        )

        def callback(future):
            if not future.exception():
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_PUTITEM_END,
                    dict(
                        put_request=put_request,
                        if_not_exist=if_not_exist,
                        expected_condition_map=expected_condition_map
                    )
                )

            else:
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_DELETEITEM_ERROR,
                    payload=future.exception(),
                    priority=notifier.PRIORITY_ERROR
                )

        put_future.add_done_callback(callback)
        return put_future

    def delete_item(self, context, delete_request,
                    expected_condition_map=None):
        with self.__task_semaphore:
            result = self._storage_driver.delete_item(context, delete_request,
                                                      expected_condition_map)
            notifier.notify(
                context, notifier.EVENT_TYPE_DATA_DELETEITEM,
                dict(
                    delete_request=delete_request,
                    expected_condition_map=expected_condition_map
                ),
                priority=notifier.PRIORITY_DEBUG
            )

            return result

    def delete_item_async(self, context, delete_request,
                          expected_condition_map=None):
        notifier.notify(
            context, notifier.EVENT_TYPE_DATA_DELETEITEM_START,
            dict(
                delete_request=delete_request,
                expected_condition_map=expected_condition_map
            )
        )

        del_future = self._execute_async(
            self._storage_driver.delete_item,
            context, delete_request, expected_condition_map
        )

        def callback(future):
            if not future.exception():
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_DELETEITEM_END,
                    dict(
                        delete_request=delete_request,
                        expected_condition_map=expected_condition_map
                    )
                )

            else:
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_DELETEITEM_ERROR,
                    future.exception(), priority=notifier.PRIORITY_ERROR
                )

        del_future.add_done_callback(callback)
        return del_future

    def execute_write_batch(self, context, write_request_list):
        assert write_request_list

        unprocessed_items = []

        request_count = len(write_request_list)
        done_count = [0]

        done_event = Event()

        notifier.notify(context, notifier.EVENT_TYPE_DATA_BATCHWRITE_START,
                        write_request_list)

        for req in write_request_list:
            if isinstance(req, models.PutItemRequest):
                future_result = self.put_item_async(context, req)
            elif isinstance(req, models.DeleteItemRequest):
                future_result = self.delete_item_async(context, req)
            else:
                assert False, 'Wrong WriteItemRequest'

            def callback(res):
                try:
                    res.result()
                except Exception:
                    unprocessed_items.append(req)
                    LOG.exception("Can't process WriteItemRequest")
                done_count[0] += 1
                if done_count[0] >= request_count:
                    done_event.set()

            future_result.add_done_callback(callback)

        done_event.wait()

        notifier.notify(
            context, notifier.EVENT_TYPE_DATA_BATCHWRITE_END,
            dict(
                write_request_list=write_request_list,
                unprocessed_items=unprocessed_items
            )
        )

        return unprocessed_items

    def execute_get_batch(self, context, read_request_list):
        assert read_request_list

        items = []
        unprocessed_items = []

        request_count = len(read_request_list)
        done_count = [0]

        done_event = Event()

        notifier.notify(context, notifier.EVENT_TYPE_DATA_BATCHREAD_START,
                        read_request_list)

        for req in read_request_list:
            future_result = self.select_item_async(context,
                                                   req.table_name,
                                                   req.indexed_condition_map,
                                                   req.select_type,
                                                   consistent=req.consistent)

            def make_callback(req):
                def callback(res):
                    try:
                        items.append((req.table_name, res.result()))
                    except Exception:
                        unprocessed_items.append(req)
                        LOG.exception("Can't process GetItemRequest")
                    done_count[0] += 1
                    if done_count[0] >= request_count:
                        done_event.set()
                return callback

            future_result.add_done_callback(make_callback(req))

        done_event.wait()

        notifier.notify(
            context, notifier.EVENT_TYPE_DATA_BATCHREAD_END,
            dict(
                read_request_list=read_request_list,
                unprocessed_items=unprocessed_items
            )
        )

        return items, unprocessed_items

    def update_item(self, context, table_name, key_attribute_map,
                    attribute_action_map, expected_condition_map=None):
        with self.__task_semaphore:
            result = self._storage_driver.update_item(
                context, table_name, key_attribute_map, attribute_action_map,
                expected_condition_map
            )
            notifier.notify(
                context, notifier.EVENT_TYPE_DATA_UPDATEITEM,
                dict(
                    table_name=table_name,
                    key_attribute_map=key_attribute_map,
                    attribute_action_map=attribute_action_map,
                    expected_condition_map=expected_condition_map
                ),
                priority=notifier.PRIORITY_DEBUG
            )

            return result

    def select_item(self, context, table_name, indexed_condition_map=None,
                    select_type=None, index_name=None, limit=None,
                    exclusive_start_key=None, consistent=True,
                    order_type=None):
        with self.__task_semaphore:
            result = self._storage_driver.select_item(
                context, table_name, indexed_condition_map, select_type,
                index_name, limit, exclusive_start_key, consistent, order_type
            )
            notifier.notify(
                context, notifier.EVENT_TYPE_DATA_SELECTITEM,
                dict(
                    table_name=table_name,
                    indexed_condition_map=indexed_condition_map,
                    select_type=select_type,
                    index_name=index_name,
                    limit=limit,
                    exclusive_start_key=exclusive_start_key,
                    consistent=consistent,
                    order_type=order_type
                ),
                priority=notifier.PRIORITY_DEBUG
            )

            return result

    def select_item_async(self, context, table_name,
                          indexed_condition_map=None,
                          select_type=None, index_name=None, limit=None,
                          exclusive_start_key=None, consistent=True,
                          order_type=None):
        payload = dict(table_name=table_name,
                       indexed_condition_map=indexed_condition_map,
                       select_type=select_type,
                       index_name=index_name,
                       limit=limit,
                       exclusive_start_key=exclusive_start_key,
                       consistent=consistent,
                       order_type=order_type)
        notifier.notify(context, notifier.EVENT_TYPE_DATA_SELECTITEM_START,
                        payload)
        result = self._execute_async(
            self._storage_driver.select_item,
            context, table_name, indexed_condition_map, select_type,
            index_name, limit, exclusive_start_key, consistent, order_type
        )
        notifier.notify(context, notifier.EVENT_TYPE_DATA_SELECTITEM_END,
                        payload)
        return result

    def scan(self, context, table_name, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None,
             consistent=False):
        with self.__task_semaphore:
            payload = dict(table_name=table_name,
                           condition_map=condition_map,
                           attributes_to_get=attributes_to_get,
                           limit=limit,
                           exclusive_start_key=exclusive_start_key,
                           consistent=consistent)
            notifier.notify(context, notifier.EVENT_TYPE_DATA_SCAN_START,
                            payload)
            result = self._storage_driver.scan(
                context, table_name, condition_map, attributes_to_get,
                limit, exclusive_start_key, consistent
            )
            notifier.notify(context, notifier.EVENT_TYPE_DATA_SCAN_END,
                            payload)

            return result
