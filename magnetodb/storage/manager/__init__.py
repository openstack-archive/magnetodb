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


class StorageManager(object):
    def create_table(self, tenant, table_name, table_schema):
        """
        Create table

        :param tenant: tenant for table
        :param table_name: String, name of the table to create
        :param table_schema: TableSchema instance which define table to create

        :returns: TableMeta instance with metadata of created table

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def delete_table(self, tenant, table_name):
        """
        Delete table

        :param tenant: tenant for table
        :param table_name: String, name of table to delete

        :returns: TableMeta instance with metadata of created table

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def describe_table(self, tenant, table_name):
        """
        Describe table

        :param tenant: tenant for table
        :param table_name: String, name of table to describes

        :returns: TableMeta instance

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def list_tables(self, tenant, exclusive_start_table_name=None,
                    limit=None):
        """
        :param tenant: tenant for table
        :param exclusive_start_table_name:
        :param limit: limit of returned table names
        :returns: list of table names

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def list_all_tables(self, last_evaluated_project=None,
                        last_evaluated_table=None, limit=None):
        """
        :param last_evaluated_project: last evaluated project id
        :param last_evaluated_table: last evaluated table name
        :param limit: limit of returned list size
        :returns: list of tenant names and tables

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def put_item(self, tenant, table_name, attribute_map, return_values=None,
                 if_not_exist=False, expected_condition_map=None):
        """
        :param tenant: tenant for table
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
        raise NotImplementedError()

    def put_item_async(self, tenant, table_name, attribute_map,
                       return_values=None, if_not_exist=False,
                       expected_condition_map=None):
        """
        :param tenant: tenant for table
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
        raise NotImplementedError()

    def delete_item(self, tenant, table_name, key_attribute_map,
                    expected_condition_map=None):
        """
        :param tenant: tenant for table
        :param table_name: name of the table
        :param key_attribute_map: attribute name to AttributeValue
                    instance map, which represents key to identify item
                    to delete
        :param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be deleted
                    or not

        :returns: True if operation performed, otherwise False (if operation
                    was skipped by out of date timestamp, it is considered as
                    successfully performed)

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def delete_item_async(self, tenant, table_name, key_attribute_map,
                          expected_condition_map=None):
        """
        :param tenant: tenant for table
        :param table_name: name of the table
        :param key_attribute_map: attribute name to AttributeValue
                    instance map, which represents key to identify item
                    to delete
        :param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be deleted
                    or not

        :returns: Future instance

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def execute_write_batch(self, tenant, write_request_map):
        """
        :param tenant: tenant for table
        :param write_request_map: table name to WriteItemRequest
                instance list map to execute batch operation

        :returns: Unprocessed request list
        """
        raise NotImplementedError()

    def execute_get_batch(self, tenant, get_request_list):
        """
        :param tenant: tenant for table
        :param get_request_list: contains GetItemRequest instances to execute
                    batch operation

        :returns: tuple of items list and unprocessed request list
        """
        raise NotImplementedError()

    def update_item(self, tenant, table_name, key_attribute_map,
                    attribute_action_map, expected_condition_map=None):
        """
        :param tenant: tenant for table
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
        raise NotImplementedError()

    def query(self, tenant, table_name, indexed_condition_map,
              select_type=None, index_name=None, limit=None,
              exclusive_start_key=None, consistent=True, order_type=None):
        """
        :param tenant: tenant for table
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
        raise NotImplementedError()

    def get_item(self, tenant, table_name, key_attribute_map,
                 select_type=None, consistent=True):
        """
        :param tenant: tenant for table
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
        raise NotImplementedError()

    def scan(self, tenant, table_name, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None,
             consistent=False):
        """
        :param tenant: tenant for table
        :param table_name: String, name of table to get item from
        :param condition_map: attribute name to
                    ScanCondition instance mapping. It defines rows
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
        raise NotImplementedError()

    def get_table_statistics(self, tenant, table_info, keys):
        """
        :param tenant: tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param keys: list of metrics

        :returns: count of items in table and table size

        :raises: BackendInteractionException
        """
        raise NotImplementedError()
