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
from magnetodb.common.exception import ValidationError

from magnetodb import notifier
from magnetodb.openstack.common.gettextutils import _

from magnetodb.storage.models import SelectType
from magnetodb.storage.models import IndexedCondition
from magnetodb.storage.models import PutItemRequest
from magnetodb.storage.models import DeleteItemRequest
from magnetodb.storage.models import TableMeta

from magnetodb.storage.manager import StorageManager

from magnetodb.storage.table_info_repo import TableInfo

LOG = logging.getLogger(__name__)


class SimpleStorageManager(StorageManager):

    def __init__(self, storage_driver, table_info_repo, concurrent_tasks=1000):
        self._storage_driver = storage_driver
        self._table_info_repo = table_info_repo
        self.__task_executor = ThreadPoolExecutor(concurrent_tasks)
        self.__task_semaphore = BoundedSemaphore(concurrent_tasks)

    def create_table(self, context, table_name, table_schema):
        table_info = TableInfo(table_name, table_schema,
                               TableMeta.TABLE_STATUS_CREATING)
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_START,
                        table_schema)

        try:
            self._table_info_repo.save(context, table_info)
        except TableAlreadyExistsException as e:
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_ERROR,
                            e.message, priority=notifier.PRIORITY_ERROR)
            raise

        try:
            table_info.internal_name = self._storage_driver.create_table(
                context, table_info
            )
            table_info.status = TableMeta.TABLE_STATUS_ACTIVE
            self._table_info_repo.update(
                context, table_info, ["status", "internal_name"]
            )
        except BackendInteractionException as ex:
            notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_ERROR,
                            ex.message, priority=notifier.PRIORITY_ERROR)
            raise

        notifier.notify(context, notifier.EVENT_TYPE_TABLE_CREATE_END,
                        table_schema)

        return TableMeta(table_info.schema, table_info.status)

    def delete_table(self, context, table_name):
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_START,
                        table_name)

        table_info = self._table_info_repo.get(context, table_name)

        table_info.status = TableMeta.TABLE_STATUS_DELETING

        self._table_info_repo.update(context, table_info, ["status"])

        self._storage_driver.delete_table(context, table_info)

        self._table_info_repo.delete(context, table_name)

        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DELETE_END,
                        table_name)

        return TableMeta(table_info.schema, table_info.status)

    def describe_table(self, context, table_name):
        table_info = self._table_info_repo.get(context, table_name, ['status'])
        notifier.notify(context, notifier.EVENT_TYPE_TABLE_DESCRIBE,
                        table_name, priority=notifier.PRIORITY_DEBUG)

        return TableMeta(table_info.schema, table_info.status)

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

    @staticmethod
    def _validate_table_is_active(table_info):
        if table_info.status != TableMeta.TABLE_STATUS_ACTIVE:
            raise ValidationError(
                _("Can't execute request: "
                  "Table '%(table_name)s' status '%(table_status)s' "
                  "isn't %(expected_status)s"),
                table_name=table_info.name, table_status=table_info.status,
                expected_status=TableMeta.TABLE_STATUS_ACTIVE
            )

    @staticmethod
    def _validate_key_schema(table_info, attribute_map,
                             property_name="key attributes", keys_only=True,
                             index_name=None):
        schema_key_attributes = table_info.schema.key_attributes
        schema_attribute_type_map = table_info.schema.attribute_type_map

        if index_name is None:
            required_key_attribute_names = schema_key_attributes
        else:
            required_key_attribute_names = schema_key_attributes[:]
            required_key_attribute_names.append(
                table_info.schema.index_def_map[index_name].attribute_to_index
            )

        table_schema_is_valid = True
        if keys_only:
            key_attribute_map = attribute_map
            if len(required_key_attribute_names) != len(key_attribute_map):
                table_schema_is_valid = False
            else:
                for schema_key_attr_name in required_key_attribute_names:
                    key_attribute = key_attribute_map.get(schema_key_attr_name,
                                                          None)
                    if (key_attribute is None or
                            schema_attribute_type_map[schema_key_attr_name] !=
                            key_attribute.attr_type):
                        table_schema_is_valid = False
                        break
        else:
            key_attribute_map = {}
            for schema_key_attr_name in required_key_attribute_names:
                key_attribute = attribute_map.get(schema_key_attr_name, None)
                if key_attribute is None:
                    table_schema_is_valid = False
                else:
                    key_attribute_map[schema_key_attr_name] = key_attribute
                    if (schema_attribute_type_map[schema_key_attr_name] !=
                            key_attribute.attr_type):
                        table_schema_is_valid = False

        if not table_schema_is_valid:
            raise ValidationError(
                _("Specified %(property_name)s: %(property_value)s "
                  "doesn't match table schema: %(table_schema)s"),
                property_name=property_name,
                property_value=str(key_attribute_map),
                table_schema=str(table_info.schema)
            )

    def put_item(self, context, put_request, if_not_exist=False,
                 expected_condition_map=None):
        table_info = self._table_info_repo.get(context, put_request.table_name)
        self._validate_table_is_active(table_info)
        self._validate_key_schema(table_info, put_request.attribute_map,
                                  keys_only=False)

        with self.__task_semaphore:
            result = self._storage_driver.put_item(
                context, table_info, put_request.attribute_map, if_not_exist,
                expected_condition_map
            )
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

    def _put_item_async(self, context, table_info, attribute_map,
                        if_not_exist=False, expected_condition_map=None):
        payload = dict(
            table_name=table_info.name,
            attribute_map=attribute_map,
            if_not_exist=if_not_exist,
            expected_condition_map=expected_condition_map
        )
        notifier.notify(context, notifier.EVENT_TYPE_DATA_PUTITEM_START,
                        payload)

        put_future = self._execute_async(
            self._storage_driver.put_item,
            context, table_info, attribute_map, if_not_exist,
            expected_condition_map
        )

        def callback(future):
            if not future.exception():
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_PUTITEM_END,
                    payload
                )
            else:
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_DELETEITEM_ERROR,
                    payload=future.exception(),
                    priority=notifier.PRIORITY_ERROR
                )

        put_future.add_done_callback(callback)
        return put_future

    def put_item_async(self, context, put_request, if_not_exist=False,
                       expected_condition_map=None):
        table_info = self._table_info_repo.get(context, put_request.table_name)
        self._validate_table_is_active(table_info)
        self._validate_key_schema(table_info, put_request.attribute_map,
                                  keys_only=False)

        return self._put_item_async(
            context, table_info, put_request.attribute_map, if_not_exist,
            expected_condition_map
        )

    def delete_item(self, context, delete_request,
                    expected_condition_map=None):
        table_info = self._table_info_repo.get(context,
                                               delete_request.table_name)
        self._validate_table_is_active(table_info)
        self._validate_key_schema(table_info, delete_request.key_attribute_map)

        with self.__task_semaphore:
            result = self._storage_driver.delete_item(
                context, table_info, delete_request.key_attribute_map,
                expected_condition_map
            )
        notifier.notify(
            context, notifier.EVENT_TYPE_DATA_DELETEITEM,
            dict(
                delete_request=delete_request,
                expected_condition_map=expected_condition_map
            ),
            priority=notifier.PRIORITY_DEBUG
        )

        return result

    def _delete_item_async(self, context, table_info, key_attribute_map,
                           expected_condition_map=None):
        payload = dict(
            table_name=table_info.name,
            key_attribute_map=key_attribute_map,
            expected_condition_map=expected_condition_map
        )
        notifier.notify(context, notifier.EVENT_TYPE_DATA_DELETEITEM_START,
                        payload)

        del_future = self._execute_async(
            self._storage_driver.delete_item,
            context, table_info, key_attribute_map, expected_condition_map
        )

        def callback(future):
            if not future.exception():
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_DELETEITEM_END,
                    payload
                )
            else:
                notifier.notify(
                    context, notifier.EVENT_TYPE_DATA_DELETEITEM_ERROR,
                    future.exception(), priority=notifier.PRIORITY_ERROR
                )

        del_future.add_done_callback(callback)
        return del_future

    def delete_item_async(self, context, delete_request,
                          expected_condition_map=None):
        table_info = self._table_info_repo.get(context,
                                               delete_request.table_name)
        self._validate_table_is_active(table_info)
        self._validate_key_schema(table_info, delete_request.key_attribute_map)

        return self._delete_item_async(context, table_info,
                                       delete_request.key_attribute_map,
                                       expected_condition_map)

    def execute_write_batch(self, context, write_request_list):
        assert write_request_list

        unprocessed_items = []

        request_count = len(write_request_list)
        done_count = [0]

        done_event = Event()

        notifier.notify(context, notifier.EVENT_TYPE_DATA_BATCHWRITE_START,
                        write_request_list)
        prepared_batch = []
        for req in write_request_list:
            def make_request_executor():
                _req = req

                _table_info = self._table_info_repo.get(context,
                                                        _req.table_name)
                self._validate_table_is_active(_table_info)

                _request_function = None
                _request_param = None
                if isinstance(_req, PutItemRequest):
                    _request_param = _req.attribute_map
                    self._validate_key_schema(_table_info, _request_param,
                                              keys_only=False)
                    _request_function = self._put_item_async
                elif isinstance(_req, DeleteItemRequest):
                    _request_param = _req.key_attribute_map
                    self._validate_key_schema(_table_info, _request_param)
                    _request_function = self._delete_item_async
                else:
                    assert False, (
                        'Wrong WriteItemRequest.Should never happen!!!'
                    )

                def callback(res):
                    try:
                        res.result()
                    except Exception:
                        unprocessed_items.append(_req)
                        LOG.exception("Can't process WriteItemRequest")
                    done_count[0] += 1
                    if done_count[0] >= request_count:
                        done_event.set()

                def executor():
                    future_result = _request_function(context, _table_info,
                                                      _request_param)
                    future_result.add_done_callback(callback)
                return executor

            prepared_batch.append(make_request_executor())

        for request_executor in prepared_batch:
            request_executor()

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

        prepared_batch = []

        for req in read_request_list:
            def make_request_executor():
                _req = req

                _table_name = _req.table_name
                _key_attribute_map = _req.key_attribute_map

                _table_info = self._table_info_repo.get(context, _table_name)
                self._validate_table_is_active(_table_info)
                self._validate_key_schema(_table_info, _key_attribute_map)

                _attributes_to_get = req.attributes_to_get

                def callback(res):
                    try:
                        items.append((_table_name, res.result()))
                    except Exception:
                        unprocessed_items.append(_req)
                        LOG.exception("Can't process GetItemRequest")
                    done_count[0] += 1
                    if done_count[0] >= request_count:
                        done_event.set()

                def executor():
                    future_result = self._get_item_async(
                        context, _table_info,
                        _key_attribute_map.get(
                            _table_info.schema.hash_key_name
                        ),
                        _key_attribute_map.get(
                            _table_info.schema.range_key_name
                        ),
                        _attributes_to_get, consistent=_req.consistent
                    )
                    future_result.add_done_callback(callback)
                return executor
            prepared_batch.append(make_request_executor())

        notifier.notify(context, notifier.EVENT_TYPE_DATA_BATCHREAD_START,
                        read_request_list)

        for request_executor in prepared_batch:
            request_executor()

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
        table_info = self._table_info_repo.get(context, table_name)
        self._validate_table_is_active(table_info)
        self._validate_key_schema(table_info, key_attribute_map)

        with self.__task_semaphore:
            result = self._storage_driver.update_item(
                context, table_info, key_attribute_map, attribute_action_map,
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

    def select_item(self, context, table_name, indexed_condition_map,
                    select_type, index_name=None, limit=None,
                    exclusive_start_key=None, consistent=True,
                    order_type=None):
        table_info = self._table_info_repo.get(context, table_name)
        self._validate_table_is_active(table_info)

        schema_attribute_type_map = table_info.schema.attribute_type_map

        hash_key_name = table_info.schema.hash_key_name
        range_key_name = table_info.schema.range_key_name

        if index_name is not None:
            index_def = table_info.schema.index_def_map.get(index_name)
            if index_def is None:
                raise ValidationError(
                    _("Index '%(index_name)s' doesn't exist for table "
                      "'%(table_name)s'"),
                    index_name=index_name, table_name=table_name)
            range_key_name_to_query = index_def.attribute_to_index
        else:
            range_key_name_to_query = range_key_name

        if exclusive_start_key is not None:
            self._validate_key_schema(
                table_info, exclusive_start_key,
                property_name="exclusive_start_key", index_name=index_name
            )

        indexed_condition_map_copy = indexed_condition_map.copy()

        hash_key_condition_list = indexed_condition_map_copy.pop(hash_key_name,
                                                                 None)
        range_key_to_query_condition_list = indexed_condition_map_copy.pop(
            range_key_name_to_query, None
        )

        indexed_condition_schema_valid = False
        if len(indexed_condition_map_copy) == 0 and hash_key_condition_list:
            hash_key_type = schema_attribute_type_map[hash_key_name]
            for hash_key_condition in hash_key_condition_list:
                for hash_key_condition_arg in hash_key_condition.args:
                    if hash_key_condition_arg.attr_type != hash_key_type:
                        break
                else:
                    continue
                break
            else:
                if range_key_to_query_condition_list:
                    range_key_to_query_type = schema_attribute_type_map[
                        range_key_name_to_query
                    ]
                    for range_key_to_query_condition in (
                            range_key_to_query_condition_list):
                        for range_key_to_query_condition_arg in (
                                range_key_to_query_condition.args):
                            if (range_key_to_query_condition_arg.attr_type !=
                                    range_key_to_query_type):
                                break
                        else:
                            continue
                        break
                    else:
                        indexed_condition_schema_valid = True
                else:
                    indexed_condition_schema_valid = True

        if not indexed_condition_schema_valid:
            raise ValidationError(
                _("Specified query conditions %(indexed_condition_map)s "
                  "doesn't match table schema: %(table_schema)s"),
                indexed_condition_map=str(indexed_condition_map),
                table_schema=str(table_info.schema)
            )

        if (len(hash_key_condition_list) != 1 or
                hash_key_condition_list[0].type !=
                IndexedCondition.CONDITION_TYPE_EQUAL):
            raise ValidationError(
                _("Only equality condition is allowed for HASH key attribute "
                  "'%(hash_key_name)s'"),
                hash_key_name=hash_key_name,
            )

        with self.__task_semaphore:
            result = self._storage_driver.select_item(
                context, table_info, hash_key_condition_list,
                range_key_to_query_condition_list, select_type,
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

    def _get_item_async(self, context, table_info, hash_key, range_key,
                        attributes_to_get, consistent=True):
        payload = dict(table_name=table_info.name,
                       hash_key=hash_key,
                       range_key=range_key,
                       attributes_to_get=attributes_to_get,
                       consistent=consistent)
        notifier.notify(context, notifier.EVENT_TYPE_DATA_SELECTITEM_START,
                        payload)
        select_type = (
            SelectType.all() if attributes_to_get is None else
            SelectType.specific_attributes(attributes_to_get)
        )
        hash_key_condition_list = [IndexedCondition.eq(hash_key)]
        range_key_condition_list = (
            None if range_key is None else [IndexedCondition.eq(range_key)]
        )

        result = self._execute_async(
            self._storage_driver.select_item,
            context, table_info, hash_key_condition_list,
            range_key_condition_list, select_type, consistent=consistent
        )
        notifier.notify(context, notifier.EVENT_TYPE_DATA_SELECTITEM_END,
                        payload)
        return result

    def scan(self, context, table_name, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None,
             consistent=False):
        table_info = self._table_info_repo.get(context, table_name)
        self._validate_table_is_active(table_info)

        if exclusive_start_key is not None:
            self._validate_key_schema(
                table_info, exclusive_start_key,
                property_name="exclusive_start_key"
            )

        payload = dict(table_name=table_name,
                       condition_map=condition_map,
                       attributes_to_get=attributes_to_get,
                       limit=limit,
                       exclusive_start_key=exclusive_start_key,
                       consistent=consistent)
        notifier.notify(context, notifier.EVENT_TYPE_DATA_SCAN_START,
                        payload)

        with self.__task_semaphore:
            result = self._storage_driver.scan(
                context, table_info, condition_map, attributes_to_get,
                limit, exclusive_start_key, consistent
            )
        notifier.notify(context, notifier.EVENT_TYPE_DATA_SCAN_END,
                        payload)

        return result
