# Copyright 2013 Mirantis Inc.
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

from oslo_serialization import jsonutils as json

from oslo_context import context as req_context

from magnetodb.common import config
from magnetodb.openstack.common import importutils
from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)

__STORAGE_MANAGER_IMPL = None
__BACKUP_MANAGER_IMPL = None
__RESTORE_MANAGER_IMPL = None


def process_object_spec(obj_name, obj_spec_map, context):
    if obj_name in context:
        return context[obj_name]

    obj_spec = obj_spec_map[obj_name]

    if obj_name not in obj_spec_map:
        raise AttributeError(
            "Object with name {} is not found".format(
                obj_name
            )
        )

    args = obj_spec.get("args")
    res_args = []
    if args:
        for param_value in args:
            if (isinstance(param_value, basestring) and
                    len(param_value) > 1 and param_value[0] == "@"):
                param_value = param_value[1:]
                if param_value[0] != "@":
                    param_value = process_object_spec(
                        param_value, obj_spec_map, context
                    )
            res_args.append(param_value)

    kwargs = obj_spec.get("kwargs")
    res_kwargs = {}
    if kwargs:
        for param_name, param_value in kwargs.iteritems():
            if (isinstance(param_value, basestring) and
                    len(param_value) > 1 and param_value[0] == "@"):
                param_value = param_value[1:]
                if param_value[0] != "@":
                    param_value = process_object_spec(
                        param_value, obj_spec_map, context
                    )
            res_kwargs[param_name] = param_value

    type_str = obj_spec["type"]
    mod_str, _sep, class_str = type_str.rpartition('.')
    if mod_str:
        module = importutils.import_module(mod_str)
        cls = getattr(module, class_str)
    else:
        cls = eval(type_str)
    obj = cls(*res_args, **res_kwargs)
    context[obj_name] = obj

    return obj


def load_context(config):
    storage_manager_config = config.storage_manager_config
    object_spec_map = json.loads(storage_manager_config)
    context = {}
    for object_name in object_spec_map.keys():
        process_object_spec(object_name, object_spec_map, context)
    return context


def setup():
    global __STORAGE_MANAGER_IMPL
    assert __STORAGE_MANAGER_IMPL is None

    global __BACKUP_MANAGER_IMPL
    assert __BACKUP_MANAGER_IMPL is None

    global __RESTORE_MANAGER_IMPL
    assert __RESTORE_MANAGER_IMPL is None

    context = load_context(config.CONF)

    __STORAGE_MANAGER_IMPL = context["storage_manager"]
    __BACKUP_MANAGER_IMPL = context["backup_manager"]
    __RESTORE_MANAGER_IMPL = context["restore_manager"]


def create_table(tenant, table_name, table_schema):
    """
    Creates table

    :param tenant: tenant for table creating to
    :param table_name: String, name of the table to create
    :param table_schema: TableSchema instance which define table to create

    :returns: TableMeta instance with metadata of created table

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name, table_schema=table_schema
    )
    return __STORAGE_MANAGER_IMPL.create_table(tenant, table_name,
                                               table_schema)


def delete_table(tenant, table_name):
    """
    Delete table

    :param tenant: tenant for table deleting from
    :param table_name: String, name of table to delete

    :returns: TableMeta instance with metadata of created table

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name
    )
    return __STORAGE_MANAGER_IMPL.delete_table(tenant, table_name)


def describe_table(tenant, table_name):
    """
    Describe table

    :param tenant for getting table from
    :param table_name: String, name of table to describes

    :returns: TableMeta instance

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name
    )
    return __STORAGE_MANAGER_IMPL.describe_table(tenant, table_name)


def list_tables(tenant, exclusive_start_table_name=None, limit=None):
    """
    :param tenant for getting table names from
    :param exclusive_start_table_name:
    :param limit: limit of returned table names
    :returns: list of table names

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, exclusive_start_table_name=exclusive_start_table_name,
        limit=limit
    )
    return __STORAGE_MANAGER_IMPL.list_tables(
        tenant, exclusive_start_table_name, limit
    )


def list_all_tables(last_evaluated_tenant=None,
                    last_evaluated_table=None, limit=None):
    """
    :param last_evaluated_project: last evaluated project id
    :param last_evaluated_table: last evaluated table name
    :param limit: limit of returned list size
    :returns: list of tenant id and table list

    :raises: BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.list_all_tables(
        last_evaluated_tenant, last_evaluated_table, limit
    )


def put_item(tenant, table_name, attribute_map, return_values=None,
             if_not_exist=False, expected_condition_map=None):
    """
    :param tenant: tenant for table for putting item to
    :param table_name: name of the table
    :param attribute_map: attribute name to AttributeValue instance map,
                which represents item to put
    :param return_values: model that defines what values should be returned
    :param if_not_exist: put item only is row is new record (It is possible
                to use only one of if_not_exist and expected_condition_map
                parameter)
    :param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be put or
                not

    :returns: True if operation performed, otherwise False

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name, attribute_map=attribute_map,
        return_values=return_values, if_not_exist=if_not_exist,
        expected_condition_map=expected_condition_map
    )
    return __STORAGE_MANAGER_IMPL.put_item(
        tenant, table_name, attribute_map, return_values,
        if_not_exist, expected_condition_map
    )


def put_item_async(tenant, table_name, attribute_map,
                   return_values=None, if_not_exist=False,
                   expected_condition_map=None):
    """
    :param tenant: tenant for table for putting item to
    :param table_name: name of the table
    :param attribute_map: attribute name to AttributeValue instance map,
                which represents item to put
    :param return_values: model that defines what values should be returned
    :param if_not_exist: put item only is row is new record (It is possible
                to use only one of if_not_exist and expected_condition_map
                parameter)
    :param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be put or
                not

    :returns: Future instance

    :raises: BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.put_item_async(
        tenant, table_name, attribute_map, return_values,
        if_not_exist, expected_condition_map
    )


def delete_item(tenant, table_name, key_attribute_map,
                expected_condition_map=None):
    """
    :param tenant: tenant for table for deleting item from
    :param table_name: name of the table
    :param key_attribute_map: attribute name to AttributeValue instance map,
                which represents key to identify item
                to delete
    :param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be deleted
                or not

    :returns: True if operation performed, otherwise False (if operation was
                skipped by out of date timestamp, it is considered as
                successfully performed)

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name,
        key_attribute_map=key_attribute_map,
        expected_condition_map=expected_condition_map
    )
    return __STORAGE_MANAGER_IMPL.delete_item(
        tenant, table_name, key_attribute_map, expected_condition_map
    )


def delete_item_async(tenant, table_name, key_attribute_map,
                      expected_condition_map=None):
    """
    :param tenant: tenant for table for deleting item from
    :param table_name: name of the table
    :param key_attribute_map: attribute name to AttributeValue instance map,
                which represents key to identify item
                to delete
    :param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be deleted
                or not

    :returns: Future instance

    :raises: BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.delete_item_async(
        tenant, table_name, key_attribute_map, expected_condition_map
    )


def execute_write_batch(tenant, write_request_map):
    """
    :param tenant: tenant for table for performing batch operations for
    :param write_request_map: table name to WriteItemRequest
                instance list map to execute batch operation

    :returns: Unprocessed request list
    """

    req_context.get_current().request_args = dict(
        tenant=tenant, write_request_map=write_request_map
    )
    return __STORAGE_MANAGER_IMPL.execute_write_batch(tenant,
                                                      write_request_map)


def execute_get_batch(tenant, get_request_list):
    """
    :param tenant: tenant for table for performing batch operations for
    :param get_request_list: contains get requests instances to execute
                             batch operation

    :returns: tuple of items list and unprocessed request list
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, get_request_list=get_request_list)
    return __STORAGE_MANAGER_IMPL.execute_get_batch(
        tenant, get_request_list
    )


def update_item(tenant, table_name, key_attribute_map,
                attribute_action_map, expected_condition_map=None):
    """
    :param tenant: tenant for table where item will be updated
    :param table_name: String, name of table to delete item from
    :param key_attribute_map: key attribute name to
                AttributeValue mapping. It defines row it to update item
    :param attribute_action_map: attribute name to UpdateItemAction
                instance mapping. It defines actions to perform for each
                given attribute
    :param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions
                to make decision about should item be updated or not
    :returns: True if operation performed, otherwise False

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name,
        key_attribute_map=key_attribute_map,
        attribute_action_map=attribute_action_map,
        expected_condition_map=expected_condition_map
    )
    return __STORAGE_MANAGER_IMPL.update_item(
        tenant, table_name, key_attribute_map, attribute_action_map,
        expected_condition_map
    )


def query(tenant, table_name, indexed_condition_map=None,
          select_type=None, index_name=None, limit=None,
          exclusive_start_key=None, consistent=True,
          order_type=None):
    """
    :param tenant: tenant for table for querying items from
    :param table_name: String, name of table to get item from
    :param indexed_condition_map: indexed attribute name to
                IndexedCondition instance mapping. It defines rows
                set to be selected
    :param select_type: SelectType instance. It defines with attributes
                will be returned. If not specified, default will be used:
                SelectType.all() for query on table and
                SelectType.all_projected() for query on index
    :param index_name: String, name of index to search with
    :param limit: maximum count of returned values
    :param exclusive_start_key: key attribute names to AttributeValue
                instance
    :param consistent: define is operation consistent or not (by default it
                is not consistent)
    :param order_type: defines order of returned rows, if 'None' - default
                order will be used


    :returns: SelectResult instance

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name,
        indexed_condition_map=indexed_condition_map, select_type=select_type,
        index_name=index_name, limit=limit,
        exclusive_start_key=exclusive_start_key, consistent=consistent,
        order_type=order_type
    )
    return __STORAGE_MANAGER_IMPL.query(
        tenant, table_name, indexed_condition_map, select_type,
        index_name, limit, exclusive_start_key, consistent, order_type
    )


def get_item(tenant, table_name, key_attribute_map=None,
             select_type=None, consistent=True):
    """
    :param tenant: tenant for table for getting item from
    :param table_name: String, name of table to get item from
    :param key_attribute_map: key attribute name to
                AttributeValue mapping. It defines row to get
    :param select_type: SelectType instance. It defines with attributes
                will be returned. If not specified, default will be used:
                SelectType.all() for query on table and
                SelectType.all_projected() for query on index
    :param consistent: define is operation consistent or not (by default it
                is not consistent)

    :returns: SelectResult instance

    :raises: BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.get_item(
        tenant, table_name, key_attribute_map, select_type, consistent
    )


def scan(tenant, table_name, condition_map, attributes_to_get=None,
         limit=None, exclusive_start_key=None,
         consistent=False):
    """
    :param tenant: tenant for table for scanning items from
    :param table_name: String, name of table to get item from
    :param condition_map: attribute name to
                IndexedCondition instance mapping. It defines rows
                set to be selected
    :param attributes_to_get: list of attribute names to be included in
                result. If None, all attributes will be included
    :param limit: maximum count of returned values
    :param exclusive_start_key: key attribute names to AttributeValue
                instance
    :param consistent: define is operation consistent or not (by default it
                is not consistent)

    :returns: list of attribute name to AttributeValue mappings

    :raises: BackendInteractionException
    """
    req_context.get_current().request_args = dict(
        tenant=tenant, table_name=table_name, condition_map=condition_map,
        attributes_to_get=attributes_to_get, limit=limit,
        exclusive_start_key=exclusive_start_key, consistent=consistent,
    )
    return __STORAGE_MANAGER_IMPL.scan(
        tenant, table_name, condition_map, attributes_to_get, limit,
        exclusive_start_key, consistent=False
    )


def health_check():
    """
    :returns: True

    :raises: BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.health_check()


def get_table_statistics(tenant, table_name, keys):
    """
    :param table_name: String, name of table to get item count from
    :param keys: list of metrics

    :returns: count of items in table and table size

    :raises: BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.get_table_statistics(
        tenant, table_name, keys
    )


def create_backup(tenant, table_name, backup_name, strategy):
    """
    Create backup

    :param tenant: tenant for table
    :param table_name: String, name of the table to backup
    :param backup_name: String, name of the backup to create
    :param strategy: Dict, strategy used for the backup

    :returns: BackupMeta

    :raises: BackendInteractionException
    """

    return __BACKUP_MANAGER_IMPL.create_backup(
        tenant, table_name, backup_name, strategy)


def describe_backup(tenant, table_name, backup_id):
    return __BACKUP_MANAGER_IMPL.describe_backup(
        tenant, table_name, backup_id)


def delete_backup(tenant, table_name, backup_id):
    return __BACKUP_MANAGER_IMPL.delete_backup(
        tenant, table_name, backup_id)


def list_backups(tenant, table_name,
                 exclusive_start_backup_id, limit):
    return __BACKUP_MANAGER_IMPL.list_backups(
        tenant, table_name, exclusive_start_backup_id, limit)


def create_restore_job(tenant, table_name, backup_id, source):
    """
    Create restore job

    :param tenant: tenant for table
    :param table_name: String, name of the table to restore
    :param backup_id: String, id of the backup to restore from
    :param source: String, source of the backup to restore from

    :returns: RestoreJobMeta

    :raises: BackendInteractionException
    """

    return __RESTORE_MANAGER_IMPL.create_restore_job(
        tenant, table_name, backup_id, source)


def describe_restore_job(tenant, table_name, restore_job_id):
    return __RESTORE_MANAGER_IMPL.describe_restore_job(
        tenant, table_name, restore_job_id)


def list_restore_jobs(tenant, table_name,
                      exclusive_start_restore_job_id, limit):

    return __RESTORE_MANAGER_IMPL.list_restore_jobs(
        tenant, table_name, exclusive_start_restore_job_id, limit)
