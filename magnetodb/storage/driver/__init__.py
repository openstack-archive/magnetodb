# Copyright 2014 Mirantis Inc.
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


class StorageDriver(object):
    def create_table(self, tenant, table_info):
        """
        Create table at the backend side

        :param tenant for table
        :param table_info: TableInfo instance with table's meta information

        :returns: internal_table_name created

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def delete_table(self, tenant, table_info):
        """
        Delete table from the backend side

        :param tenant for table
        :param table_info: TableInfo instance with table's meta information

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def batch_write(self, tenant, write_request_list):
        """
        Execute batch on storage backend side

        :param tenant for table
        :param write_request_list: (TableInfo, WriteItemRequest) list,
                    represents write requests set to be perform

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def put_item(self, tenant, table_info, attribute_map,
                 return_values=None, if_not_exist=False,
                 expected_condition_map=None):
        """
        :param tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param attribute_map: attribute name to AttributeValue mapping.
                    It defines row key and additional attributes to put
                    item
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

    def delete_item(self, tenant, table_info, key_attribute_map,
                    expected_condition_map=None):
        """
        :param tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param key_attribute_map: key attribute name to
                    AttributeValue mapping. It defines row to be deleted
        :param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be
                    deleted or not

        :returns: True if operation performed, otherwise False (if operation
                    was skipped by out of date timestamp, it is considered as
                    successfully performed)

        :raises: BackendInteractionException
        """
        raise NotImplementedError()

    def update_item(self, tenant, table_info, key_attribute_map,
                    attribute_action_map, expected_condition_map=None):
        """
        :param tenant for table
        :param table_info: TableInfo instance with table's meta information
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

    def select_item(self, tenant, table_info, hash_key_condition_list,
                    range_key_to_query_condition_list, select_type,
                    index_name=None, limit=None, exclusive_start_key=None,
                    consistent=True, order_type=None):
        """
        :param tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param hash_key_condition_list: list of IndexedCondition instances.
                    Defines conditions for hash key to perform query on
        :param range_key_to_query_condition_list: list of IndexedCondition
                    instances. Defines conditions for range key or indexed
                    attribute to perform query on
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

    def scan(self, tenant, table_info, condition_map,
             attributes_to_get=None, limit=None, exclusive_start_key=None,
             consistent=False):
        """
        :param tenant for table
        :param table_info: TableInfo instance with table's meta information
        :param condition_map: attribute name to list of ScanCondition
                    instances mapping. It defines rows set to be selected
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
        :param context: current request context
        :param table_info: TableInfo instance with table's meta information
        :param keys: list of metrics

        :returns: count of items in table and table size

        :raises: BackendInteractionException
        """
        raise NotImplementedError()
