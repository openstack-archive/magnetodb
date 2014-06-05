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
from threading import BoundedSemaphore, Event
from concurrent.futures import ThreadPoolExecutor, Future

from magnetodb.openstack.common.notifier import api as notifier_api

from magnetodb.common.exception import TableAlreadyExistsException
from magnetodb.common.exception import BackendInteractionException
from magnetodb.common.notifier.event import Notification
from magnetodb.common.notifier.event import notify

from magnetodb.storage import models
from magnetodb.storage.manager import StorageManager
from magnetodb.storage.table_info_repo import TableInfo

LOG = logging.getLogger(__name__)


class SimpleStorageManager(StorageManager):

    def __init__(self, storage_driver, table_info_repo, concurrent_tasks=1000,
                 publisher_id='magnetodb'):
        self._publisher_id = notifier_api.publisher_id(publisher_id)
        self._storage_driver = storage_driver
        self._table_info_repo = table_info_repo
        self.__task_executor = ThreadPoolExecutor(concurrent_tasks)
        self.__task_semaphore = BoundedSemaphore(concurrent_tasks)

    def create_table(self, context, table_name, table_schema):
        table_info = TableInfo(table_name, table_schema,
                               models.TableMeta.TABLE_STATUS_CREATING)
        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.TABLE_CREATE_START,
               priority=notifier_api.CONF.default_notification_level,
               payload=table_schema)

        try:
            self._table_info_repo.save(context, table_info)
        except TableAlreadyExistsException as e:
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.TABLE_CREATE_ERROR,
                   priority=notifier_api.ERROR,
                   payload=e.message)
            raise

        try:
            self._storage_driver.create_table(context, table_name)
            table_info.status = models.TableMeta.TABLE_STATUS_ACTIVE
            self._table_info_repo.update(
                context, table_info, ["status"]
            )
        except BackendInteractionException as ex:
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.TABLE_CREATE_ERROR,
                   priority=notifier_api.ERROR,
                   payload=ex.message)
            raise

        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.TABLE_CREATE_END,
               priority=notifier_api.CONF.default_notification_level,
               payload=table_schema)

        return models.TableMeta(table_info.schema, table_info.status)

    def delete_table(self, context, table_name):
        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.TABLE_DELETE_START,
               priority=notifier_api.CONF.default_notification_level,
               payload=table_name)

        table_info = self._table_info_repo.get(context, table_name)

        table_info.status = models.TableMeta.TABLE_STATUS_DELETING

        self._table_info_repo.update(context, table_info, ["status"])

        self._storage_driver.delete_table(context, table_name)

        self._table_info_repo.delete(context, table_name)

        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.TABLE_DELETE_END,
               priority=notifier_api.CONF.default_notification_level,
               payload=table_name)

        return models.TableMeta(table_info.schema, table_info.status)

    def describe_table(self, context, table_name):
        table_info = self._table_info_repo.get(context,
                                               table_name,
                                               ['status'])
        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.TABLE_DESCRIBE,
               priority=notifier_api.CONF.default_notification_level,
               payload=table_name)

        return models.TableMeta(table_info.schema, table_info.status)

    def list_tables(self, context, exclusive_start_table_name=None,
                    limit=None):
        tnames = self._table_info_repo.get_tenant_table_names(
            context, exclusive_start_table_name, limit
        )
        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.TABLE_LIST,
               priority=notifier_api.CONF.default_notification_level,
               payload=exclusive_start_table_name)

        return tnames

    def _execute_async(self, func, *args, **kwargs):
        def wrapper(future_result):
            try:
                future_result.set_result(func(*args, **kwargs))
            except Exception as e:
                future_result.set_exception(e)

        future = Future()

        def callback(future):
            self.__task_semaphore.release()

        future.add_done_callback(callback)
        self.__task_semaphore.acquire()
        self.__task_executor.submit(wrapper, future)
        return future

    def put_item(self, context, put_request, if_not_exist=False,
                 expected_condition_map=None):
        with self.__task_semaphore:
            result = self._storage_driver.put_item(context, put_request,
                                                   if_not_exist,
                                                   expected_condition_map)
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.DATA_PUTITEM,
                   priority=notifier_api.CONF.default_notification_level,
                   payload=dict(put_request=put_request,
                                if_not_exist=if_not_exist,
                                expected_condition_map=expected_condition_map))

            return result

    def put_item_async(self, context, put_request, if_not_exist=False,
                       expected_condition_map=None):
        ecm = expected_condition_map
        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.DATA_PUTITEM_START,
               priority=notifier_api.CONF.default_notification_level,
               payload=dict(put_request=put_request,
                            if_not_exist=if_not_exist,
                            expected_condition_map=ecm))

        put_future = self._execute_async(
            self._storage_driver.put_item,
            context, put_request, if_not_exist, ecm
        )

        def callback(future):
            if not future.exception():
                notify(context=context,
                       publisher_id=self._publisher_id,
                       event_type=Notification.DATA_PUTITEM_END,
                       priority=notifier_api.CONF.default_notification_level,
                       payload=dict(put_request=put_request,
                                    if_not_exist=if_not_exist,
                                    expected_condition_map=ecm))

            else:
                notify(context=context,
                       publisher_id=self._publisher_id,
                       event_type=Notification.DATA_DELETEITEM_ERROR,
                       priority=notifier_api.ERROR,
                       payload=future.exception())

        put_future.add_done_callback(callback)
        return put_future

    def delete_item(self, context, delete_request,
                    expected_condition_map=None):
        with self.__task_semaphore:
            result = self._storage_driver.delete_item(context, delete_request,
                                                      expected_condition_map)
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.DATA_DELETEITEM,
                   priority=notifier_api.CONF.default_notification_level,
                   payload=dict(delete_request=delete_request,
                                expected_condition_map=expected_condition_map))

            return result

    def delete_item_async(self, context, delete_request,
                          expected_condition_map=None):
        ecm = expected_condition_map
        notify(context,
               publisher_id=self._publisher_id,
               event_type=Notification.DATA_DELETEITEM_START,
               priority=notifier_api.CONF.default_notification_level,
               payload=dict(delete_request=delete_request,
                            expected_condition_map=ecm))

        del_future = self._execute_async(
            self._storage_driver.delete_item,
            context, delete_request, ecm
        )

        def callback(future):
            if not future.exception():
                notify(context,
                       publisher_id=self._publisher_id,
                       event_type=Notification.DATA_DELETEITEM_END,
                       priority=notifier_api.CONF.default_notification_level,
                       payload=dict(delete_request=delete_request,
                                    expected_condition_map=ecm))

            else:
                notify(context=context,
                       publisher_id=self._publisher_id,
                       event_type=Notification.DATA_DELETEITEM_ERROR,
                       priority=notifier_api.ERROR,
                       payload=future.exception())

        del_future.add_done_callback(callback)
        return del_future

    def execute_write_batch(self, context, write_request_list):
        assert write_request_list

        unprocessed_items = []

        request_count = len(write_request_list)
        done_count = [0]

        done_event = Event()

        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.DATA_BATCHWRITE_START,
               priority=notifier_api.CONF.default_notification_level,
               payload=write_request_list)

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

        notify(context=context,
               publisher_id=self._publisher_id,
               event_type=Notification.DATA_BATCHWRITE_END,
               priority=notifier_api.CONF.default_notification_level,
               payload=dict(write_request_list=write_request_list,
                            unprocessed_items=unprocessed_items))

        return unprocessed_items

    def update_item(self, context, table_name, key_attribute_map,
                    attribute_action_map, expected_condition_map=None):
        with self.__task_semaphore:
            result = self._storage_driver.update_item(
                context, table_name, key_attribute_map, attribute_action_map,
                expected_condition_map
            )
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.DATA_UPDATEITEM,
                   priority=notifier_api.CONF.default_notification_level,
                   payload=dict(table_name=table_name,
                                key_attribute_map=key_attribute_map,
                                attribute_action_map=attribute_action_map,
                                expected_condition_map=expected_condition_map))

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
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.DATA_SELECTITEM,
                   priority=notifier_api.CONF.default_notification_level,
                   payload=dict(table_name=table_name,
                                indexed_condition_map=indexed_condition_map,
                                select_type=select_type,
                                index_name=index_name,
                                limit=limit,
                                exclusive_start_key=exclusive_start_key,
                                consistent=consistent,
                                order_type=order_type))

            return result

    def scan(self, context, table_name, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None,
             consistent=False):
        with self.__task_semaphore:
            result = self._storage_driver.scan(
                context, table_name, condition_map, attributes_to_get,
                limit, exclusive_start_key, consistent
            )
            notify(context=context,
                   publisher_id=self._publisher_id,
                   event_type=Notification.DATA_SCAN,
                   priority=notifier_api.CONF.default_notification_level,
                   payload=dict(table_name=table_name,
                                condition_map=condition_map,
                                attributes_to_get=attributes_to_get,
                                limit=limit,
                                exclusive_start_key=exclusive_start_key,
                                consistent=consistent))

            return result
