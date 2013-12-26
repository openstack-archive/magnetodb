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

from magnetodb.common import config
from magnetodb.openstack.common import importutils

from magnetodb.openstack.common import jsonutils

__STORAGE_IMPL = None


def setup():
    global __STORAGE_IMPL
    assert __STORAGE_IMPL is None

    storage_param = jsonutils.loads(config.CONF.storage_param)

    __STORAGE_IMPL = importutils.import_class(config.CONF.storage_impl)(
        **storage_param
    )


def create_table(context, table_schema):
    """
    Creates table

    @param context: current request context
    @param table_schema: TableSchema instance which define table to create

    @raise BackendInteractionException
    """
    __STORAGE_IMPL.create_table(context, table_schema)


def delete_table(context, table_name):
    """
    Creates table

    @param context: current request context
    @param table_name: String, name of table to delete

    @raise BackendInteractionException
    """
    __STORAGE_IMPL.delete_table(context, table_name)


def describe_table(context, table_name):
    """
    Creates table

    @param context: current request context
    @param table_name: String, name of table to describes

    @return: TableSchema instance

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.describe_table(context, table_name)


def list_tables(context, exclusive_start_table_name=None, limit=None):
    """
    @param context: current request context
    @param exclusive_start_table_name
    @param limit: limit of returned table names
    @return list of table names

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.list_tables(context,
                                      exclusive_start_table_name, limit)


def put_item(context, put_request, if_not_exist=False,
             expected_condition_map=None):
    """
    @param context: current request context
    @param put_request: contains PutItemRequest items to perform
                put item operation
    @param if_not_exist: put item only is row is new record (It is possible to
                use only one of if_not_exist and expected_condition_map
                parameter)
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides preconditions
                to make decision about should item be put or not

    @return: True if operation performed, otherwise False

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.put_item(context, put_request, if_not_exist,
                                   expected_condition_map)


def delete_item(context, delete_request, expected_condition_map=None):
    """
    @param context: current request context
    @param delete_request: contains DeleteItemRequest items to perform
                delete item operation
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides preconditions
                to make decision about should item be deleted or not

    @return: True if operation performed, otherwise False (if operation was
                skipped by out of date timestamp, it is considered as
                successfully performed)

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.delete_item(context, delete_request,
                                      expected_condition_map)


def execute_write_batch(context, write_request_list, durable=True):
    """
    @param context: current request context
    @param write_request_list: contains WriteItemBatchableRequest items to
                perform batch
    @param durable: if True, batch will be fully performed or fully skipped.
                Partial batch execution isn't allowed

    @raise BackendInteractionException
    """
    __STORAGE_IMPL.execute_write_batch(context, write_request_list, durable)


def update_item(context, table_name, key_attribute_map, attribute_action_map,
                expected_condition_map=None):
    """
    @param context: current request context
    @param table_name: String, name of table to delete item from
    @param key_attribute_map: key attribute name to
                AttributeValue mapping. It defines row it to update item
    @param attribute_action_map: attribute name to UpdateItemAction instance
                mapping. It defines actions to perform for each given attribute
    @param expected_condition_map: expected attribute name to
                ExpectedCondition instance mapping. It provides preconditions
                to make decision about should item be updated or not
    @return: True if operation performed, otherwise False

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.update_item(context, table_name, key_attribute_map,
                                      attribute_action_map,
                                      expected_condition_map)


def select_item(context, table_name, indexed_condition_map, select_type=None,
                index_name=None, limit=None, exclusive_start_key=None,
                consistent=True, order_type=None):
    """
    @param context: current request context
    @param table_name: String, name of table to get item from
    @param indexed_condition_map: indexed attribute name to
                IndexedCondition instance mapping. It defines rows
                set to be selected
    @param select_type: SelectType instance. It defines with attributes will be
                returned. If not specified, default will be used:
                    SelectType.all() for query on table and
                    SelectType.all_projected() for query on index
    @param index_name: String, name of index to search with
    @param limit: maximum count of returned values
    @param exclusive_start_key: key attribute names to AttributeValue instance
    @param consistent: define is operation consistent or not (by default it is
                not consistent)
    @param order_type: defines order of returned rows, if 'None' - default
                order will be used

    @return SelectResult instance

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.select_item(
        context, table_name, indexed_condition_map, select_type,
        index_name, limit, exclusive_start_key, consistent, order_type
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
    @param attributes_to_get: list of attribute names to be included in result.
                if None, all attributes will be included
    @param limit: maximum count of returned values
    @param exclusive_start_key: key attribute names to AttributeValue instance
    @param consistent: define is operation consistent or not (by default it is
                not consistent)

    @return list of attribute name to AttributeValue mappings

    @raise BackendInteractionException
    """
    return __STORAGE_IMPL.scan(
        context, table_name, condition_map,
        attributes_to_get=attributes_to_get,
        limit=limit, exclusive_start_key=exclusive_start_key,
        consistent=consistent
    )
