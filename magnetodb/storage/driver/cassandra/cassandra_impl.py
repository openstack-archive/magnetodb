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

from magnetodb.common import exception
from magnetodb.openstack.common import log as logging
from magnetodb.storage import models
from magnetodb.storage.driver import StorageDriver
from magnetodb.storage.driver.cassandra import USER_PREFIX
from magnetodb.storage.driver.cassandra import SYSTEM_COLUMN_EXTRA_ATTR_DATA
from magnetodb.storage.driver.cassandra import SYSTEM_COLUMN_EXTRA_ATTR_TYPES
from magnetodb.storage.driver.cassandra import query_builder
from magnetodb.storage.models import AttributeValue

LOG = logging.getLogger(__name__)

USER_PREFIX_LENGTH = len(USER_PREFIX)


def _decode_predefined_attr(table_info, cas_name, cas_val, prefix=USER_PREFIX):
    assert cas_name.startswith(prefix) and cas_val

    name = cas_name[USER_PREFIX_LENGTH:]
    storage_type = table_info.schema.attribute_type_map[name]

    return name, AttributeValue(storage_type, cas_val)


def _decode_dynamic_value(value, storage_type):
    value = json.loads(value)

    return AttributeValue(storage_type, encoded_value=value)


def _compact_indexed_condition(cond_list):
    left_condition = None
    right_condition = None
    exact_condition = None

    assert cond_list

    for condition in cond_list:
        if condition.type == models.IndexedCondition.CONDITION_TYPE_EQUAL:
            if (exact_condition is not None and
                    condition.arg.value != exact_condition.arg.value):
                return None
            exact_condition = condition
        elif condition.is_left_border():
            if left_condition is None:
                left_condition = condition
            elif condition.is_strict_border():
                if (condition.arg.value >=
                        left_condition.arg.value):
                    left_condition = condition
            else:
                if (condition.arg.value >
                        left_condition.arg.value):
                    left_condition = condition
        elif condition.is_right_border():
            if right_condition is None:
                right_condition = condition
            elif condition.is_strict():
                if (condition.arg.value <=
                        right_condition.arg.value):
                    right_condition = condition
            else:
                if (condition.arg.value <
                        right_condition.arg.value):
                    right_condition = condition

    if exact_condition is not None:
        if left_condition is not None:
            if left_condition.is_strict():
                if (left_condition.arg.value >=
                        exact_condition.arg.value):
                    return None
            else:
                if (left_condition.arg.value >
                        exact_condition.arg.value):
                    return None
        if right_condition is not None:
            if right_condition.is_strict():
                if (right_condition.arg.value <=
                        exact_condition.arg.value):
                    return None
            else:
                if (right_condition.arg.value <
                        exact_condition.arg.value):
                    return None
        return [exact_condition]
    elif left_condition is not None:
        if right_condition is not None:
            if (left_condition.is_strict_border() or
                    right_condition.is_strict_border()):
                if (left_condition.arg.value >=
                        right_condition.arg.value):
                    return None
            else:
                if (left_condition.arg.value >
                        right_condition.arg.value):
                    return None
            return [left_condition, right_condition]
        else:
            return [left_condition]

    assert right_condition is not None

    return [right_condition]


class CassandraStorageDriver(StorageDriver):
    def __init__(self, cluster_handler, table_info_repo):
        self.__cluster_handler = cluster_handler
        self.__table_info_repo = table_info_repo

    def create_table(self, context, table_name):
        table_info = self.__table_info_repo.get(context, table_name)
        table_schema = table_info.schema

        cas_table_name = USER_PREFIX + table_name
        cas_keyspace = USER_PREFIX + context.tenant

        key_count = len(table_schema.key_attributes)

        if key_count < 1 or key_count > 2:
            raise exception.BackendInteractionException(
                "Expected 1 or 2 key attribute(s). Found {}: {}".format(
                    key_count, table_schema.key_attributes))

        query = query_builder.generate_create_table_query(
            cas_keyspace, cas_table_name, table_schema
        )

        self.__cluster_handler.execute_query(query)
        LOG.debug("Create Table CQL request executed. "
                  "Waiting for schema agreement...")

        self.__cluster_handler.wait_for_table_status(
            keyspace_name=cas_keyspace, table_name=cas_table_name,
            expected_exists=True)
        LOG.debug("Waiting for schema agreement... Done")

        table_info.internal_name = cas_table_name
        self.__table_info_repo.update(
            context, table_info, ["internal_name"]
        )

    def delete_table(self, context, table_name):
        table_info = self.__table_info_repo.get(context, table_name)

        cas_table_name = table_info.internal_name
        cas_keyspace_name = USER_PREFIX + context.tenant

        query = 'DROP TABLE "{}"."{}"'.format(cas_keyspace_name,
                                              cas_table_name)

        self.__cluster_handler.execute_query(query)

        LOG.debug("Delete Table CQL request executed. "
                  "Waiting for schema agreement...")

        self.__cluster_handler.wait_for_table_status(
            keyspace_name=cas_keyspace_name, table_name=cas_table_name,
            expected_exists=False
        )

        LOG.debug("Waiting for schema agreement... Done")

    def _put_item_if_not_exists(self, table_info, attribute_map):
        put_query = query_builder.generate_put_query(
            table_info, attribute_map, if_not_exist=True
        )
        result = self.__cluster_handler.execute_query(put_query,
                                                      consistent=True)
        return result[0]['[applied]']

    def put_item(self, context, put_request, if_not_exist=False,
                 expected_condition_map=None):
        """
        @param context: current request context
        @param put_request: contains PutItemRequest items to perform
                    put item operation
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

        table_info = self.__table_info_repo.get(context,
                                                put_request.table_name)

        if if_not_exist:
            if expected_condition_map:
                raise exception.BackendInteractionException(
                    "Specifying expected_condition_map and"
                    "if_not_exist is not allowed both"
                )
        if table_info.schema.index_def_map:
            while True:
                old_indexes = self._select_current_index_values(
                    table_info, put_request.attribute_map
                )

                put_request = query_builder.generate_put_query(
                    table_info, put_request.attribute_map,
                    old_indexes=old_indexes,
                    if_not_exist=if_not_exist or old_indexes is None,
                    expected_condition_map=expected_condition_map
                )
                result = self.__cluster_handler.execute_query(put_request,
                                                              consistent=True)

                if result[0]['[applied]']:
                    return True

                if old_indexes is None:
                    if if_not_exist:
                        return False
                    else:
                        continue
                else:
                    for attr_name, attr_value in old_indexes.iteritems():
                        cas_name = USER_PREFIX + attr_name
                        (_, current_value) = _decode_predefined_attr(
                            table_info, cas_name, result[0][cas_name])
                        if current_value != attr_value:
                            # index consistency condition wasn't passed
                            break
                    else:
                        # expected condition wasn't passed
                        return False
        else:
            put_query = query_builder.generate_put_query(
                table_info, put_request.attribute_map,
                if_not_exist=if_not_exist,
                expected_condition_map=expected_condition_map
            )
            result = self.__cluster_handler.execute_query(put_query,
                                                          consistent=True)
            return (result is None) or result[0]['[applied]']

    def _select_current_index_values(
            self, table_info, attribute_map):
        select_query = (
            query_builder.generate_select_current_index_values_query(
                table_info, attribute_map
            )
        )

        select_result = self.__cluster_handler.execute_query(select_query,
                                                             consistent=False)
        if not select_result:
            return None

        assert len(select_result) == 1
        index_values = {}

        for cas_attr_name, cas_attr_value in select_result[0].iteritems():
            if cas_attr_value:
                attr_name = cas_attr_name[USER_PREFIX_LENGTH:]
                attr_type = table_info.schema.attribute_type_map[
                    attr_name
                ]
                index_values[attr_name] = (
                    AttributeValue(attr_type, cas_attr_value)
                )
        return index_values

    def delete_item(self, context, delete_request,
                    expected_condition_map=None):
        """
        @param context: current request context
        @param delete_request: contains DeleteItemRequest items to perform
                    delete item operation
        @param expected_condition_map: expected attribute name to
                    ExpectedCondition instance mapping. It provides
                    preconditions to make decision about should item be deleted
                    or not

        @return: True if operation performed, otherwise False (if operation was
                    skipped by out of date timestamp, it is considered as
                    successfully performed)

        @raise BackendInteractionException
        """

        table_info = self.__table_info_repo.get(context,
                                                delete_request.table_name)

        if table_info.schema.index_def_map:
            while True:
                old_indexes = self._select_current_index_values(
                    table_info, delete_request.key_attribute_map
                )

                if old_indexes is None:
                    # Nothing to delete
                    return not expected_condition_map

                delete_query = query_builder.generate_delete_query(
                    table_info, delete_request.key_attribute_map, old_indexes,
                    expected_condition_map
                )

                result = self.__cluster_handler.execute_query(delete_query,
                                                              consistent=True)

                if result[0]['[applied]']:
                    return True

                for attr_name, attr_value in old_indexes.iteritems():
                    cas_name = USER_PREFIX + attr_name
                    (_, current_value) = _decode_predefined_attr(
                        table_info, cas_name, result[0][cas_name])
                    if current_value != attr_value:
                        # index consistency condition wasn't passed
                        break
                else:
                    # expected condition wasn't passed
                    return False
        else:
            delete_query = query_builder.generate_delete_query(
                table_info, delete_request.key_attribute_map,
                expected_condition_map=expected_condition_map
            )
            result = self.__cluster_handler.execute_query(delete_query,
                                                          consistent=True)
            return (result is None) or result[0]['[applied]']

    def update_item(self, context, table_name, key_attribute_map,
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
                    preconditions to make decision about should item be updated
                    or not
        @return: True if operation performed, otherwise False

        @raise BackendInteractionException
        """
        attribute_action_map = attribute_action_map or {}

        table_info = self.__table_info_repo.get(context, table_name)

        if table_info.schema.index_def_map:
            index_actions = {}
            for index_name, index_def in (
                    table_info.schema.index_def_map.iteritems()):
                attr_name = index_def.attribute_to_index
                action = attribute_action_map.get(
                    attr_name, None
                )
                if action:
                    index_actions[attr_name] = action

            while True:
                old_indexes = self._select_current_index_values(
                    table_info, key_attribute_map
                )

                if old_indexes is None:
                    if expected_condition_map:
                        return False

                    attribute_map = key_attribute_map.copy()
                    for attr_name, attr_action in (
                            attribute_action_map.iteritems()):
                        if attr_action.action in (
                                models.UpdateItemAction.UPDATE_ACTION_PUT,
                                models.UpdateItemAction.UPDATE_ACTION_ADD):
                            attribute_map[attr_name] = attr_action.value
                    if self._put_item_if_not_exists(table_info,
                                                    attribute_map):
                        return True
                    else:
                        continue

                attribute_map = key_attribute_map.copy()
                for attr_name, attr_action in (
                        attribute_action_map.iteritems()):
                    if attr_action.action in (
                            models.UpdateItemAction.UPDATE_ACTION_PUT,
                            models.UpdateItemAction.UPDATE_ACTION_ADD):
                        attribute_map[attr_name] = attr_action.value
                    else:
                        attribute_map[attr_name] = None

                update_query = query_builder.generate_update_query(
                    table_info, attribute_map, old_indexes,
                    expected_condition_map=None
                )
                result = self.__cluster_handler.execute_query(update_query,
                                                              consistent=True)

                if result[0]['[applied]']:
                    return True

                for attr_name, attr_value in old_indexes.iteritems():
                    cas_name = USER_PREFIX + attr_name
                    (_, current_value) = _decode_predefined_attr(
                        table_info, cas_name, result[0][cas_name])
                    if current_value != attr_value:
                        # index consistency condition wasn't passed
                        break
                else:
                    # expected condition wasn't passed
                    return False
        else:
            attribute_map = key_attribute_map.copy()
            for attr_name, attr_action in (
                    attribute_action_map.iteritems()):
                if attr_action.action in (
                        models.UpdateItemAction.UPDATE_ACTION_PUT,
                        models.UpdateItemAction.UPDATE_ACTION_ADD):
                    attribute_map[attr_name] = attr_action.value
                else:
                    attribute_map[attr_name] = None

            update_query = query_builder.generate_update_query(
                table_info, attribute_map, expected_condition_map=None
            )
            result = self.__cluster_handler.execute_query(update_query,
                                                          consistent=True)

            return (result is None) or result[0]['[applied]']

    def select_item(self, context, table_name, indexed_condition_map=None,
                    select_type=models.SelectType.all(), index_name=None,
                    limit=None,
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

        table_info = self.__table_info_repo.get(context, table_name)

        assert (
            not index_name or (
                table_info.schema.index_def_map and
                index_name in table_info.schema.index_def_map
            )
        ), "index_name '{}' isn't specified in the schema".format(
            index_name
        )

        hash_name = table_info.schema.key_attributes[0]

        range_name = (
            table_info.schema.key_attributes[1]
            if len(table_info.schema.key_attributes) > 1
            else None
        )

        indexed_attr_name = table_info.schema.index_def_map[
            index_name
        ].attribute_to_index if index_name else None

        hash_key_cond_list = []
        index_attr_cond_list = []
        range_condition_list = []

        if indexed_condition_map:
            indexed_condition_map_copy = indexed_condition_map.copy()
            # Extracting conditions
            if hash_name in indexed_condition_map_copy:
                hash_key_cond_list = indexed_condition_map_copy.pop(hash_name)

            if index_name and (
                    indexed_attr_name in indexed_condition_map_copy):
                index_attr_cond_list = indexed_condition_map_copy.pop(
                    indexed_attr_name
                )
            if range_name and range_name in indexed_condition_map_copy:
                range_condition_list = indexed_condition_map_copy.pop(
                    range_name
                )
            assert not indexed_condition_map_copy

        #processing exclusive_start_key and append conditions
        if exclusive_start_key:
            exclusive_start_key_copy = exclusive_start_key.copy()
            exclusive_hash_key_value = exclusive_start_key_copy.pop(
                hash_name, None
            )
            if exclusive_hash_key_value:
                hash_key_cond_list.append(
                    models.IndexedCondition.eq(exclusive_hash_key_value)
                    if range_name else
                    models.IndexedCondition.gt(exclusive_hash_key_value)
                )

            if index_name:
                exclusive_indexed_value = exclusive_start_key_copy.pop(
                    indexed_attr_name
                )
                index_attr_cond_list.append(
                    models.IndexedCondition.le(exclusive_indexed_value)
                    if order_type == models.ORDER_TYPE_DESC else
                    models.IndexedCondition.ge(exclusive_indexed_value)
                )

            if range_name:
                exclusive_range_value = exclusive_start_key_copy.pop(
                    range_name
                )

                range_condition_list.append(
                    models.IndexedCondition.lt(exclusive_range_value)
                    if order_type == models.ORDER_TYPE_DESC else
                    models.IndexedCondition.gt(exclusive_range_value)
                )
            assert not exclusive_start_key_copy

        if hash_key_cond_list:
            hash_key_cond_list = _compact_indexed_condition(
                hash_key_cond_list
            )
            if not hash_key_cond_list:
                return models.SelectResult()
        if range_condition_list:
            range_condition_list = _compact_indexed_condition(
                range_condition_list
            )
            if not range_condition_list:
                return models.SelectResult()
        if index_attr_cond_list:
            index_attr_cond_list = _compact_indexed_condition(
                index_attr_cond_list
            )
            if not index_attr_cond_list:
                return models.SelectResult()

        select_type = select_type or models.SelectType.all()

        select_query = query_builder.generate_select_query(
            table_info, hash_key_cond_list, range_condition_list, index_name,
            index_attr_cond_list, select_type, limit, order_type
        )

        rows = self.__cluster_handler.execute_query(select_query, consistent)

        if select_type.is_count:
            return models.SelectResult(count=rows[0]['count'])

        # process results

        result = []

        # TODO ikhudoshyn: if select_type.is_all_projected,
        # get list of projected attrs by index_name from metainfo

        attributes_to_get = select_type.attributes

        for row in rows:
            record = {}

            #add predefined attributes
            for cas_name, cas_val in row.iteritems():
                if cas_name.startswith(USER_PREFIX) and cas_val:
                    name, val = _decode_predefined_attr(table_info, cas_name,
                                                        cas_val)
                    if not attributes_to_get or name in attributes_to_get:
                        record[name] = val

            #add dynamic attributes (from SYSTEM_COLUMN_ATTR_DATA dict)
            types = row[SYSTEM_COLUMN_EXTRA_ATTR_TYPES]
            attrs = row[SYSTEM_COLUMN_EXTRA_ATTR_DATA] or {}
            for name, val in attrs.iteritems():
                if not attributes_to_get or name in attributes_to_get:
                    typ = types[name]
                    storage_type = models.AttributeType(typ)
                    record[name] = _decode_dynamic_value(val, storage_type)

            result.append(record)

        count = len(result)
        if limit and count == limit:
            hash_name = table_info.schema.key_attributes[0]

            last_evaluated_key = {hash_name: result[-1][hash_name]}

            if len(table_info.schema.key_attributes) > 1:
                range_name = table_info.schema.key_attributes[1]
                last_evaluated_key[range_name] = result[-1][range_name]

            if index_name:
                indexed_attr_name = table_info.schema.index_def_map[
                    index_name
                ].attribute_to_index
                last_evaluated_key[indexed_attr_name] = result[-1][
                    indexed_attr_name
                ]
        else:
            last_evaluated_key = None

        return models.SelectResult(items=result,
                                   last_evaluated_key=last_evaluated_key,
                                   count=count)

    def scan(self, context, table_name, condition_map, attributes_to_get=None,
             limit=None, exclusive_start_key=None, consistent=False):
        """
        @param context: current request context
        @param table_name: String, name of table to get item from
        @param condition_map: indexed attribute name to
                    IndexedCondition instance mapping. It defines rows
                    set to be selected
        @param limit: maximum count of returned values
        @param exclusive_start_key: key attribute names to AttributeValue
                    instance
        @param consistent: define is operation consistent or not (by default it
                    is not consistent)

        @return list of attribute name to AttributeValue mappings

        @raise BackendInteractionException
        """
        if not condition_map:
            condition_map = {}

        table_info = self.__table_info_repo.get(context, table_name)

        hash_name = table_info.schema.key_attributes[0]
        try:
            range_name = table_info.schema.key_attributes[1]
        except IndexError:
            range_name = None

        key_conditions = {
            hash_name: []
        }

        if range_name:
            key_conditions[range_name] = []

        if hash_name in condition_map:
            key_conditions[hash_name] = condition_map[hash_name]

            if (range_name and range_name in condition_map
                and condition_map[range_name].type in
                    models.IndexedCondition._allowed_types):

                key_conditions[range_name] = condition_map[range_name]
        if exclusive_start_key:
            if range_name:
                key_conditions[hash_name].append(
                    models.IndexedCondition.eq(exclusive_start_key[hash_name])
                )
                key_conditions[range_name].append(
                    models.IndexedCondition.gt(exclusive_start_key[range_name])
                )
            else:
                key_conditions[hash_name].append(
                    models.IndexedCondition.gt(exclusive_start_key[hash_name])
                )

        selected = self.select_item(context, table_name, key_conditions,
                                    models.SelectType.all(), limit=limit,
                                    consistent=consistent)

        if (range_name and exclusive_start_key
                and range_name in exclusive_start_key
                and (not limit or limit > selected.count)):

            del key_conditions[range_name][-1]
            del key_conditions[hash_name][-1]
            key_conditions[hash_name].append(
                models.IndexedCondition.gt(exclusive_start_key[hash_name])
            )

            limit2 = limit - selected.count if limit else None

            selected2 = self.select_item(
                context, table_name, key_conditions,
                models.SelectType.all(), limit=limit2,
                consistent=consistent)

            selected = models.SelectResult(
                items=selected.items + selected2.items,
                last_evaluated_key=selected2.last_evaluated_key,
                count=selected.count + selected2.count
            )

        scanned_count = selected.count

        if selected.items:
            filtered_items = filter(
                lambda item: self._conditions_satisfied(
                    item, condition_map),
                selected.items)
            count = len(filtered_items)
        else:
            filtered_items = []
            count = selected.count

        if attributes_to_get and filtered_items:
            for item in filtered_items:
                for attr in item.keys():
                    if not attr in attributes_to_get:
                        del item[attr]

        filtered = models.ScanResult(
            items=filtered_items,
            last_evaluated_key=selected.last_evaluated_key,
            count=count, scanned_count=scanned_count)

        return filtered

    def _conditions_satisfied(self, row, cond_map=None):
        if not cond_map:
            return True

        for attr_name, cond_list in cond_map.iteritems():
            for cond in cond_list:
                if not self._condition_satisfied(
                        row.get(attr_name, None), cond):
                    return False
        return True

    @staticmethod
    def _condition_satisfied(attr_val, cond):

        if cond.type == models.ExpectedCondition.CONDITION_TYPE_EXISTS:
            return cond.arg == bool(attr_val)

        if not attr_val:
            return False

        if cond.type == models.Condition.CONDITION_TYPE_EQUAL:
            return (attr_val.type == cond.arg.type and
                    attr_val.value == cond.arg.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_LESS:
            return (attr_val.type == cond.arg.type and
                    attr_val.value < cond.arg.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL:
            return (attr_val.type == cond.arg.type and
                    attr_val.value <= cond.arg.value)

        if cond.type == models.IndexedCondition.CONDITION_TYPE_GREATER:
            return (attr_val.type == cond.arg.type and
                    attr_val.value > cond.arg.value)

        if (cond.type ==
                models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL):
            return (attr_val.type == cond.arg.type and
                    attr_val.value >= cond.arg.value)

        if cond.type == models.ScanCondition.CONDITION_TYPE_NOT_EQUAL:
            return (attr_val.type != cond.arg.type or
                    attr_val.value != cond.arg.value)

        if cond.type == models.ScanCondition.CONDITION_TYPE_CONTAINS:
            assert not cond.arg.type.collection_type
            if attr_val.type.element_type != cond.arg.type.element_type:
                return False

            return cond.arg.value in attr_val.value

        if cond.type == models.ScanCondition.CONDITION_TYPE_NOT_CONTAINS:
            assert not cond.arg.type.collection_type
            if attr_val.type.element_type != cond.arg.type.element_type:
                return False

            return cond.arg.value not in attr_val.value

        if cond.type == models.ScanCondition.CONDITION_TYPE_IN:
            return attr_val in cond.arg

        return False
