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
import json

from magnetodb.common import config

from magnetodb.openstack.common import importutils

from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)

__STORAGE_MANAGER_IMPL = None


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

    params = obj_spec["params"]
    res_params = {}
    for param_name, param_value in params.iteritems():
        if (isinstance(param_value, basestring) and
                len(param_value) > 1 and param_value[0] == "@"):
            param_value = param_value[1:]
            if param_value[0] != "@":
                param_value = process_object_spec(
                    param_value, obj_spec_map, context
                )
        res_params[param_name] = param_value

    type_str = obj_spec["type"]
    mod_str, _sep, class_str = type_str.rpartition('.')
    if mod_str:
        module = importutils.import_module(mod_str)
        cls = getattr(module, class_str)
    else:
        cls = eval(type_str)
    obj = cls(**res_params)
    context[obj_name] = obj

    return obj


def setup():
    global __STORAGE_MANAGER_IMPL
    assert __STORAGE_MANAGER_IMPL is None

    storage_manager_config = config.CONF.storage_manager_config

    object_spec_map = json.loads(storage_manager_config)

    context = {}

    for object_name in object_spec_map.keys():
        process_object_spec(object_name, object_spec_map, context)

    __STORAGE_MANAGER_IMPL = context["storage_manager"]


def create_table(context, table_name, table_schema):
    """
    Creates table

    @param context: current request context
    @param table_name: String, name of the table to create
    @param table_schema: TableSchema instance which define table to create

    @return TableMeta instance with metadata of created table

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.create_table(context, table_name,
                                               table_schema)


def delete_table(context, table_name):
    """
    Delete table

    @param context: current request context
    @param table_name: String, name of table to delete

    @return TableMeta instance with metadata of created table

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.delete_table(context, table_name)


def describe_table(context, table_name):
    """
    Describe table

    @param context: current request context
    @param table_name: String, name of table to describes

    @return: TableMeta instance

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.describe_table(context, table_name)


def list_tables(context, exclusive_start_table_name=None, limit=None):
    """
    @param context: current request context
    @param exclusive_start_table_name
    @param limit: limit of returned table names
    @return list of table names

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.list_tables(
        context, exclusive_start_table_name, limit
    )


def put_item(context, put_request, if_not_exist=False,
             expected_condition_map=None):
    """
    @param context: current request context
    @param put_request: PutItemRequest instance which specifies item to put
    @param if_not_exist: put item only is row is new record (It is possible
                to use only one of if_not_exist and expected_condition_map
                parameter)
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be put or
                not

    @return: True if operation performed, otherwise False

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.put_item(
        context, put_request, if_not_exist, expected_condition_map
    )


def put_item_async(context, put_request, if_not_exist=False,
                   expected_condition_map=None):
    """
    @param context: current request context
    @param put_request: PutItemRequest instance which specifies item to put
    @param if_not_exist: put item only is row is new record (It is possible
                to use only one of if_not_exist and expected_condition_map
                parameter)
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be put or
                not

    @return: Future instance

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.put_item_async(
        context, put_request, if_not_exist, expected_condition_map
    )


def delete_item(context, delete_request, expected_condition_map=None):
    """
    @param context: current request context
    @param delete_request: DeleteItemRequest instance which identifies item
                to delete
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be deleted
                or not

    @return: True if operation performed, otherwise False (if operation was
                skipped by out of date timestamp, it is considered as
                successfully performed)

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.delete_item(
        context, delete_request, expected_condition_map
    )


def delete_item_async(context, delete_request,
                      expected_condition_map=None):
    """
    @param context: current request context
    @param delete_request: DeleteItemRequest instance which identifies item
                to delete
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions to make decision about should item be deleted
                or not

    @return: Future instance

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.delete_item_async(
        context, delete_request, expected_condition_map
    )


def execute_write_batch(context, write_request_list):
    """
    @param context: current request context
    @param write_request_list: contains PutItemRequest or DeleteItemRequest
                instances to execute batch operation

    @return: Unprocessed request list
    """
    return __STORAGE_MANAGER_IMPL.execute_write_batch(context,
                                                      write_request_list)


def update_item(context, table_name, key_attribute_map,
                attribute_action_map, expected_condition_map=None):
    """
    @param context: current request context
    @param table_name: String, name of table to delete item from
    @param key_attribute_map: key attribute name to
                AttributeValue mapping. It defines row it to update item
    @param attribute_action_map: attribute name to UpdateItemAction
                instance mapping. It defines actions to perform for each
                given attribute
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides
                preconditions
                to make decision about should item be updated or not
    @return: True if operation performed, otherwise False

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.update_item(
        context, table_name, key_attribute_map, attribute_action_map,
        expected_condition_map
    )


def select_item(context, table_name, indexed_condition_map=None,
                select_type=None, index_name=None, limit=None,
                exclusive_start_key=None, consistent=True,
                order_type=None):
    """
    @param context: current request context
    @param table_name: String, name of table to get item from
    @param indexed_condition_map: indexed attribute name to
                IndexedCondition instance mapping. It defines rows
                set to be selected
    @param select_type: SelectType instance. It defines with attributes
                will be returned. If not specified, default will be used:
                    SelectType.all() for query on table and
                    SelectType.all_projected() for query on index
    @param index_name: String, name of index to search with
    @param limit: maximum count of returned values
    @param exclusive_start_key: key attribute names to AttributeValue
                instance
    @param consistent: define is operation consistent or not (by default it
                is not consistent)
    @param order_type: defines order of returned rows, if 'None' - default
                order will be used

    @return SelectResult instance

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.select_item(
        context, table_name, indexed_condition_map, select_type, index_name,
        limit, exclusive_start_key, consistent, order_type
    )


def scan(context, table_name, condition_map, attributes_to_get=None,
         limit=None, exclusive_start_key=None,
         consistent=False):
    """
    @param context: current request context
    @param table_name: String, name of table to get item from
    @param condition_map: attribute name to
                IndexedCondition instance mapping. It defines rows
                set to be selected
    @param attributes_to_get: list of attribute names to be included in
                result. If None, all attributes will be included
    @param limit: maximum count of returned values
    @param exclusive_start_key: key attribute names to AttributeValue
                instance
    @param consistent: define is operation consistent or not (by default it
                is not consistent)

    @return list of attribute name to AttributeValue mappings

    @raise BackendInteractionException
    """
    return __STORAGE_MANAGER_IMPL.scan(
        context, table_name, condition_map, attributes_to_get, limit,
        exclusive_start_key, consistent=False
    )
