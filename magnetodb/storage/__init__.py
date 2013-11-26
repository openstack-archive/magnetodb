# vim: tabstop=4 shiftwidth=4 softtabstop=4

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

CONF = config.CONF

from magnetodb.openstack.common import importutils

STORAGE_IMPL = importutils.import_module(CONF.storage_impl)


def create_table(context, table_name, attribute_defs, key_attributes,
                 indexed_non_key_attributes):
    """
    Creates table

    @param context: current request context
    @param table_name: String, name of table to create
    @param attribute_defs: list of AttributeDefinition which define table
                            attribute names and types
    @param key_attrs: list of key attribute names, contains partitional_key
                       (the first in list, required) attribute name and extra
                       key attribute names (the second and other list items,
                       not required)

            indexed_non_key_attributes: list non key names to be indexed
    """
    STORAGE_IMPL.create_table(context, table_name, attribute_defs,
                              key_attributes, indexed_non_key_attributes=None)


def list_tables(context, exclusive_start_table_name=None, limit=None):
    """
    @param context: current request context
    @param exclusive_start_table_name
    @param limit: limit of returned table names
    @return list of table names
    """
    return STORAGE_IMPL.list_tables(context, exclusive_start_table_name, limit)


# TODO (dukhlov): DynamoDB API also allows returning of deleted item as result
# of delete_item request. Now it is planned to implement this feature with 2
# requests. So if backend support this feature it would be better to extend
# this method to support it too
def delete_item(context, table_name, key_value, pre_condition_map=None):
    """
    @param context: current request context
    @param table_name: String, name of table to delete item from
    @param key_value: key attributes name to value mapping, which represents
                       full item primary key
    @param pre_condition_map: attribute name to PreCondition on this attribute
                               mapping operation will be performed only if all
                               conditions are passed
    """
    STORAGE_IMPL.list_tables(context, key_value, pre_condition_map)


def get_item(context, table_name, key_value, attributes_to_get=None,
             consistent=True):
    """
    @param context: current request context
    @param table_name: String, name of table to get item from
    @param key_value: key attributes name to value mapping, which represents
                       full item primary key
    @param attributes_to_get: attribute name list to get. If not specified, all
                               attributes should be returned
    @param consistent: define is operation consistent or not (by default it is
                        not consistent)
    @return map of retrieved attributes and it's values
    """
    return STORAGE_IMPL.list_tables(context, key_value, attributes_to_get,
                                    consistent)


# TODO (dukhlov): DynamoDB API also allows returning of old item as result
# of put_item request. Now it is planned to implement this feature with 2
# requests. So if backend support this feature it would be better to extend
# this method to support it too
def put_item(context, table_name, key_value, attributes_map,
             pre_condition_map=None):
    """
    @param context: current request context
    @param table_name: String, name of table to put item to
    @param key_value: key attributes name to value mapping, which represents
                       full item primary key
    @param attributes_map: attribute name to AttributeValue mapping.
    @param pre_condition_map: attribute name to PreCondition on this attribute
                               mapping operation will be performed only if all
                               conditions are passed
    """
    STORAGE_IMPL.list_tables(context, key_value, attributes_map,
                             pre_condition_map)


# TODO:
#DescribeTable
#Query
#Scan
#UpdateItem
#UpdateTable
