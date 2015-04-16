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

from concurrent import futures
import logging
import threading
import time
import weakref
import uuid

from oslo_context import context as req_context
from oslo_utils import timeutils

from magnetodb.common import exception
from magnetodb.common.utils import request_context_decorator

from magnetodb.i18n import _
from magnetodb import notifier
from magnetodb.storage import manager
from magnetodb.storage import models
from magnetodb.storage import table_info_repo

LOG = logging.getLogger(__name__)


class SimpleStorageManager(manager.StorageManager):

    def __init__(self, storage_driver, table_info_repo, concurrent_tasks=1000,
                 batch_chunk_size=25, schema_operation_timeout=300):
        self._storage_driver = storage_driver
        self._table_info_repo = table_info_repo
        self._batch_chunk_size = batch_chunk_size
        self._schema_operation_timeout = schema_operation_timeout
        self.__task_executor = futures.ThreadPoolExecutor(concurrent_tasks)
        self.__task_semaphore = threading.BoundedSemaphore(concurrent_tasks)
        self._notifier = notifier.get_notifier()

    def _do_create_table(self, tenant, table_info):
        start_time = time.time()
        try:
            table_info.internal_name = self._storage_driver.create_table(
                tenant, table_info
            )
        except exception.BackendInteractionError as ex:
            table_info.status = models.TableMeta.TABLE_STATUS_CREATE_FAILED
            self._table_info_repo.update(tenant, table_info, ["status"])

            self._notifier.error(
                req_context.get_current(),
                notifier.EVENT_TYPE_TABLE_CREATE_ERROR,
                dict(
                    tenant=tenant,
                    table_name=table_info.name,
                    message=ex.message,
                    value=start_time
                ))
            raise

        table_info.status = models.TableMeta.TABLE_STATUS_ACTIVE
        self._table_info_repo.update(
            tenant, table_info, ["status", "internal_name"]
        )

        self._notifier.audit(
            req_context.get_current(),
            notifier.EVENT_TYPE_TABLE_CREATE,
            dict(
                tenant=tenant,
                table_name=table_info.name,
                schema=table_info.schema,
                value=start_time
            ))

    def create_table(self, tenant, table_name, table_schema):
        table_id = uuid.uuid1()
        table_info = table_info_repo.TableInfo(
            table_name, table_id, table_schema,
            models.TableMeta.TABLE_STATUS_CREATING
        )

        try:
            self._table_info_repo.save(tenant, table_info)
        except exception.TableAlreadyExistsException:
            raise

        self._do_create_table(tenant, table_info)

        return models.TableMeta(
            table_info.id,
            table_info.schema,
            table_info.status,
            table_info.creation_date_time)

    def _do_delete_table(self, tenant, table_info):
        start_time = time.time()

        try:
            self._storage_driver.delete_table(tenant, table_info)
        except exception.BackendInteractionError as ex:
            table_info.status = models.TableMeta.TABLE_STATUS_DELETE_FAILED
            self._table_info_repo.update(tenant, table_info,
                                         ["status"])

            self._notifier.error(
                req_context.get_current(),
                notifier.EVENT_TYPE_TABLE_DELETE_ERROR,
                dict(
                    tenant=tenant,
                    table_name=table_info.name,
                    message=ex.message,
                    value=start_time
                ))
            raise

        self._table_info_repo.delete(tenant, table_info.name)

        self._notifier.audit(
            req_context.get_current(),
            notifier.EVENT_TYPE_TABLE_DELETE,
            dict(
                tenant=tenant,
                table_name=table_info.name,
                value=start_time
            )
        )

    def delete_table(self, tenant, table_name):
        try:
            table_info = self._table_info_repo.get(tenant, table_name,
                                                   ['status'])
        except exception.TableNotExistsException:
            raise

        if table_info.status == models.TableMeta.TABLE_STATUS_DELETING:
            # table is already being deleted, just return immediately
            return models.TableMeta(table_info.id, table_info.schema,
                                    table_info.status,
                                    table_info.creation_date_time)
        elif table_info.in_use:
            raise exception.ResourceInUseException()

        table_info.status = models.TableMeta.TABLE_STATUS_DELETING

        self._table_info_repo.update(tenant, table_info, ["status"])

        if not table_info.internal_name:
            # if table internal name is missing, table is not actually created
            # just remove the table_info entry for the table and
            # send notification
            msg = ("Table '{}' with tenant id '{}', id '{}' does not have "
                   "valid internal name. Unable or no need to delete."
                   ).format(table_info.name, tenant, table_info.id)
            LOG.info(msg)
            self._table_info_repo.delete(tenant, table_info.name)

            self._notifier.info(
                req_context.get_current(),
                notifier.EVENT_TYPE_TABLE_DELETE,
                dict(
                    tenant=tenant,
                    table_name=table_name,
                    message=msg,
                    value=time.time()
                ))
        else:
            self._do_delete_table(tenant, table_info)

        return models.TableMeta(
            table_info.id,
            table_info.schema,
            table_info.status,
            table_info.creation_date_time)

    def describe_table(self, tenant, table_name):
        table_info = self._table_info_repo.get(
            tenant, table_name, ['status', 'last_update_date_time'])

        if timeutils.is_older_than(table_info.last_update_date_time,
                                   self._schema_operation_timeout):
            if table_info.status == models.TableMeta.TABLE_STATUS_CREATING:
                table_info.status = models.TableMeta.TABLE_STATUS_CREATE_FAILED
                self._table_info_repo.update(tenant, table_info, ['status'])
                LOG.debug(
                    "Table '{}' creation timed out."
                    " Setting status to {}".format(
                        table_info.name,
                        models.TableMeta.TABLE_STATUS_CREATE_FAILED)
                )

            if table_info.status == models.TableMeta.TABLE_STATUS_DELETING:
                table_info.status = models.TableMeta.TABLE_STATUS_DELETE_FAILED
                self._table_info_repo.update(tenant, table_info,
                                             ['status'])
                LOG.debug(
                    "Table '{}' deletion timed out for tenant '{}'."
                    " Setting status to {}".format(
                        table_info.name, tenant,
                        models.TableMeta.TABLE_STATUS_DELETE_FAILED)
                )

        return models.TableMeta(
            table_info.id,
            table_info.schema,
            table_info.status,
            table_info.creation_date_time)

    def list_tables(self, tenant, exclusive_start_table_name=None, limit=None):
        return self._table_info_repo.list_tables(
            tenant, exclusive_start_table_name, limit
        )

    def list_all_tables(self, last_evaluated_tenant=None,
                        last_evaluated_table=None, limit=None):
        return self._table_info_repo.list_all_tables(
            last_evaluated_tenant, last_evaluated_table, limit
        )

    def _execute_async(self, func, *args, **kwargs):
        weak_self = weakref.proxy(self)
        weak_context = weakref.proxy(req_context.get_current())

        def callback(future):
            weak_self.__task_semaphore.release()

        self.__task_semaphore.acquire()
        future = self.__task_executor.submit(
            request_context_decorator.context_update_store_wrapper,
            weak_context, func, *args, **kwargs
        )
        future.add_done_callback(callback)
        return future

    @staticmethod
    def _validate_table_is_active(table_info):
        if table_info.status != models.TableMeta.TABLE_STATUS_ACTIVE:
            raise exception.ValidationError(
                _("Can't execute request: "
                  "Table '%(table_name)s' status '%(table_status)s' "
                  "isn't %(expected_status)s"),
                table_name=table_info.name, table_status=table_info.status,
                expected_status=models.TableMeta.TABLE_STATUS_ACTIVE
            )

    @staticmethod
    def _validate_table_schema(table_info, attribute_map, keys_only=True,
                               index_name=None):
        schema_key_attributes = table_info.schema.key_attributes
        schema_attribute_type_map = table_info.schema.attribute_type_map

        key_attribute_names_to_find = set(schema_key_attributes)
        if index_name is not None:
            key_attribute_names_to_find.add(
                table_info.schema.index_def_map[index_name].alt_range_key_attr
            )

        if keys_only and (
                len(key_attribute_names_to_find) != len(attribute_map)):
            raise exception.ValidationError(
                _("Specified key: %(key_attributes)s doesn't match expected "
                  "key attributes set: %(expected_key_attributes)s"),
                key_attributes=attribute_map,
                expected_key_attributes=key_attribute_names_to_find
            )

        for attr_name, typed_attr_value in attribute_map.iteritems():
            schema_attr_type = schema_attribute_type_map.get(attr_name, None)
            if schema_attr_type is None:
                continue
            key_attribute_names_to_find.discard(attr_name)

            if schema_attr_type != typed_attr_value.attr_type:
                raise exception.ValidationError(
                    _("Attribute: '%(attr_name)s' of type: '%(attr_type)s' "
                      "doesn't match table schema expected attribute type: "
                      "'%(expected_attr_type)s'"),
                    attr_name=attr_name,
                    attr_type=typed_attr_value.attr_type.type,
                    expected_attr_type=schema_attr_type.type
                )

        if key_attribute_names_to_find:
            raise exception.ValidationError(
                _("Couldn't find expected key attributes: "
                  "'%(expected_key_attributes)s'"),
                expected_key_attributes=key_attribute_names_to_find
            )

    def put_item(self, tenant, table_name, attribute_map,
                 return_values=None, if_not_exist=False,
                 expected_condition_map=None):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)
        self._validate_table_schema(table_info, attribute_map,
                                    keys_only=False)

        with self.__task_semaphore:
            result = self._storage_driver.put_item(
                tenant, table_info, attribute_map, return_values,
                if_not_exist, expected_condition_map
            )

        return result

    def _put_item_async(self, tenant, table_info, attribute_map,
                        return_values=None, if_not_exist=False,
                        expected_condition_map=None):
        put_future = self._execute_async(
            self._storage_driver.put_item,
            tenant, table_info, attribute_map, return_values,
            if_not_exist, expected_condition_map
        )

        return put_future

    def put_item_async(self, tenant, table_name, attribute_map,
                       return_values, if_not_exist=False,
                       expected_condition_map=None):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)
        self._validate_table_schema(table_info, attribute_map, keys_only=False)

        return self._put_item_async(
            tenant, table_info, attribute_map, return_values,
            if_not_exist, expected_condition_map
        )

    def delete_item(self, tenant, table_name, key_attribute_map,
                    expected_condition_map=None):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)
        self._validate_table_schema(table_info, key_attribute_map)

        with self.__task_semaphore:
            result = self._storage_driver.delete_item(
                tenant, table_info, key_attribute_map, expected_condition_map
            )
        return result

    def _delete_item_async(self, tenant, table_info,
                           key_attribute_map, expected_condition_map=None):
        del_future = self._execute_async(
            self._storage_driver.delete_item, tenant, table_info,
            key_attribute_map, expected_condition_map
        )

        return del_future

    def delete_item_async(self, tenant, table_name, key_attribute_map,
                          expected_condition_map=None):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)
        self._validate_table_schema(table_info, key_attribute_map)

        return self._delete_item_async(tenant, table_info,
                                       key_attribute_map,
                                       expected_condition_map)

    @staticmethod
    def _key_values(table_info, attribute_map):
        return [
            attribute_map[key].decoded_value
            for key in table_info.schema.key_attributes
        ]

    def execute_write_batch(self, tenant, write_request_map):
        write_request_list_to_send = []
        for table_name, write_request_list in write_request_map.iteritems():
            table_info = self._table_info_repo.get(tenant, table_name)

            requested_keys = set()

            for req in write_request_list:
                self._validate_table_is_active(table_info)

                if req.is_put:
                    self._validate_table_schema(table_info, req.attribute_map,
                                                keys_only=False)
                else:
                    self._validate_table_schema(table_info, req.attribute_map)

                key_values = self._key_values(table_info, req.attribute_map)

                keys = tuple(key_values)

                if keys in requested_keys:
                    raise exception.ValidationError(
                        _("Can't execute request: "
                          "More than one operation requested"
                          " for item with keys %(keys)s"
                          " in table '%(table_name)s'"),
                        table_name=table_info.name, keys=keys
                    )

                requested_keys.add(keys)

                write_request_list_to_send.append(
                    (table_info, req)
                )

        future_result_list = []
        for i in xrange(0, len(write_request_list_to_send),
                        self._batch_chunk_size):
            req_list = (
                write_request_list_to_send[i:i+self._batch_chunk_size]
            )

            future_result_list.append(
                self._batch_write_async(tenant, req_list)
            )

        unprocessed_items = {}
        for future_result in future_result_list:
            unprocessed_request_list = future_result.result()
            for (table_info, write_request) in unprocessed_request_list:
                table_name = table_info.name
                tables_unprocessed_items = (
                    unprocessed_items.get(table_name, None)
                )
                if tables_unprocessed_items is None:
                    tables_unprocessed_items = []
                    unprocessed_items[
                        table_name
                    ] = tables_unprocessed_items

                tables_unprocessed_items.append(write_request)

        return unprocessed_items

    def _batch_write_async(self, tenant, write_request_list):
        future_result = futures.Future()

        batch_future = self._execute_async(
            self._storage_driver.batch_write,
            tenant, write_request_list
        )

        def callback(res):
            try:
                res.result()
                unprocessed_items = ()
            except NotImplementedError:
                unprocessed_items = self._batch_write_in_emulation_mode(
                    tenant, write_request_list
                )
            except Exception:
                LOG.exception("Can't process batch write request")
                unprocessed_items = write_request_list
            future_result.set_result(unprocessed_items)

        batch_future.add_done_callback(callback)

        return future_result

    def _batch_write_in_emulation_mode(self, tenant, write_request_list):
        request_count = len(write_request_list)
        done_count = [0]
        done_event = threading.Event()
        unprocessed_items = []
        for write_request in write_request_list:
            table_info, req = write_request
            if req.is_put:
                future_result = self._put_item_async(
                    tenant, table_info, req.attribute_map
                )
            elif req.is_delete:
                future_result = self._delete_item_async(
                    tenant, table_info, req.attribute_map
                )

            def make_callback():
                _write_request = write_request

                def callback(res):
                    try:
                        res.result()
                    except Exception:
                        unprocessed_items.append(_write_request)
                        LOG.exception("Can't process WriteItemRequest")
                    done_count[0] += 1
                    if done_count[0] >= request_count:
                        done_event.set()
                return callback

            future_result.add_done_callback(make_callback())

        done_event.wait()
        return unprocessed_items

    def execute_get_batch(self, tenant, read_request_list):
        assert read_request_list

        items = []
        unprocessed_items = []

        request_count = len(read_request_list)
        done_count = [0]

        done_event = threading.Event()

        prepared_batch = []

        for req in read_request_list:
            def make_request_executor():
                _req = req

                _table_name = _req.table_name
                _key_attribute_map = _req.key_attribute_map

                _table_info = self._table_info_repo.get(tenant, _table_name)
                self._validate_table_is_active(_table_info)
                self._validate_table_schema(_table_info, _key_attribute_map)

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
                        tenant, _table_info,
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

        for request_executor in prepared_batch:
            request_executor()

        done_event.wait()

        return items, unprocessed_items

    def update_item(self, tenant, table_name, key_attribute_map,
                    attribute_action_map, expected_condition_map=None):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)
        self._validate_table_schema(table_info, key_attribute_map)

        with self.__task_semaphore:
            result = self._storage_driver.update_item(
                tenant, table_info, key_attribute_map,
                attribute_action_map, expected_condition_map
            )

        return result

    @staticmethod
    def _raise_condition_schema_mismatch(condition_map, table_info):
        raise exception.ValidationError(
            _("Specified query conditions %(indexed_condition_map)s "
              "don't match table schema: %(table_schema)s"),
            indexed_condition_map=condition_map,
            table_schema=table_info.schema
        )

    def query(self, tenant, table_name, indexed_condition_map,
              select_type, index_name=None, limit=None,
              exclusive_start_key=None, consistent=True,
              order_type=None):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)

        schema_attribute_type_map = table_info.schema.attribute_type_map

        condition_map = indexed_condition_map.copy()

        # validate hash key condition

        hash_key_name = table_info.schema.hash_key_name

        hash_key_condition_list = condition_map.pop(hash_key_name, None)

        if not hash_key_condition_list:
            self._raise_condition_schema_mismatch(
                indexed_condition_map, table_info)

        if (len(hash_key_condition_list) != 1 or
            hash_key_condition_list[0].type !=
                models.IndexedCondition.CONDITION_TYPE_EQUAL):
            raise exception.ValidationError(
                _("Only equality condition is allowed for HASH key attribute "
                  "'%(hash_key_name)s'"),
                hash_key_name=hash_key_name,
            )

        hash_key_type = schema_attribute_type_map[hash_key_name]
        if hash_key_condition_list[0].arg.attr_type != hash_key_type:
            self._raise_condition_schema_mismatch(
                indexed_condition_map, table_info)

        # validate range key conditions

        range_key_name = table_info.schema.range_key_name

        if index_name is not None:
            index_def = table_info.schema.index_def_map.get(index_name)
            if index_def is None:
                raise exception.ValidationError(
                    _("Index '%(index_name)s' doesn't exist for table "
                      "'%(table_name)s'"),
                    index_name=index_name, table_name=table_name)
            range_key_name = index_def.alt_range_key_attr

        range_condition_list = condition_map.pop(range_key_name, None)
        if range_key_name:
            range_key_type = schema_attribute_type_map[range_key_name]
            range_condition_list = range_condition_list or []

            for range_condition in range_condition_list:
                if range_condition.arg.attr_type != range_key_type:
                    self._raise_condition_schema_mismatch(
                        indexed_condition_map, table_info)

        # validate extra conditions

        if len(condition_map) > 0:
            self._raise_condition_schema_mismatch(
                indexed_condition_map, table_info)

        # validate exclusive start key

        if exclusive_start_key is not None:
            self._validate_table_schema(
                table_info, exclusive_start_key, index_name=index_name
            )

        with self.__task_semaphore:
            result = self._storage_driver.select_item(
                tenant, table_info, hash_key_condition_list,
                range_condition_list, select_type,
                index_name, limit, exclusive_start_key, consistent, order_type
            )

        return result

    def get_item(self, tenant, table_name, key_attribute_map,
                 select_type, consistent=True):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)

        self._validate_table_schema(table_info, key_attribute_map)

        hash_key_name = table_info.schema.hash_key_name
        hash_key_value = key_attribute_map[hash_key_name]
        hash_key_condition_list = [
            models.IndexedCondition.eq(hash_key_value)]

        range_key_name = table_info.schema.range_key_name

        range_key_value = (
            key_attribute_map[range_key_name]
            if range_key_name else None
        )

        range_condition_list = (
            [models.IndexedCondition.eq(range_key_value)]
            if range_key_value else None
        )

        with self.__task_semaphore:
            result = self._storage_driver.select_item(
                tenant, table_info, hash_key_condition_list,
                range_condition_list, select_type,
                consistent=consistent
            )

        return result

    def _get_item_async(self, tenant, table_info, hash_key, range_key,
                        attributes_to_get, consistent=True):
        select_type = (
            models.SelectType.all() if attributes_to_get is None else
            models.SelectType.specific_attributes(attributes_to_get)
        )
        hash_key_condition_list = [models.IndexedCondition.eq(hash_key)]
        range_key_condition_list = (
            None if range_key is None
            else [models.IndexedCondition.eq(range_key)]
        )

        result = self._execute_async(
            self._storage_driver.select_item,
            tenant, table_info, hash_key_condition_list,
            range_key_condition_list, select_type, consistent=consistent
        )
        return result

    def scan(self, tenant, table_name, condition_map,
             attributes_to_get=None, limit=None, exclusive_start_key=None,
             consistent=False):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)

        if exclusive_start_key is not None:
            self._validate_table_schema(table_info, exclusive_start_key)

        with self.__task_semaphore:
            result = self._storage_driver.scan(
                tenant, table_info, condition_map, attributes_to_get,
                limit, exclusive_start_key, consistent
            )

        return result

    def health_check(self):
        return self._storage_driver.health_check()

    def get_table_statistics(self, tenant, table_name, keys):
        table_info = self._table_info_repo.get(tenant, table_name)
        self._validate_table_is_active(table_info)

        return self._storage_driver.get_table_statistics(
            tenant, table_info, keys
        )
