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

from oslo_serialization import jsonutils as json

from magnetodb.api import validation
from magnetodb.common import exception
from magnetodb.i18n import _
from magnetodb.storage import models


class Props():
    TABLE_ID = "table_id"
    TABLE_NAME = "table_name"
    ATTRIBUTE_DEFINITIONS = "attribute_definitions"
    ATTRIBUTE_NAME = "attribute_name"
    ATTRIBUTE_TYPE = "attribute_type"
    KEY_SCHEMA = "key_schema"
    KEY_TYPE = "key_type"
    LOCAL_SECONDARY_INDEXES = "local_secondary_indexes"
    GLOBAL_SECONDARY_INDEXES = "global_secondary_indexes"
    INDEX_NAME = "index_name"
    PROJECTION = "projection"
    NON_KEY_ATTRIBUTES = "non_key_attributes"
    PROJECTION_TYPE = "projection_type"

    TABLE_DESCRIPTION = "table_description"
    TABLE = "table"
    TABLE_SIZE_BYTES = "table_size_bytes"
    TABLE_STATUS = "table_status"
    CREATION_DATE_TIME = "creation_date_time"
    INDEX_SIZE_BYTES = "index_size_bytes"
    ITEM_COUNT = "item_count"

    TABLE_NAMES = "table_names"
    EXCLUSIVE_START_TABLE_NAME = "exclusive_start_table_name"
    LAST_EVALUATED_TABLE_NAME = "last_evaluated_table_name"
    LIMIT = "limit"

    EXPECTED = "expected"
    EXISTS = "exists"
    VALUE = "value"
    ITEM = "item"
    RETURN_VALUES = "return_values"
    TIME_TO_LIVE = "time_to_live"

    ATTRIBUTES = "attributes"
    ITEM_COLLECTION_METRICS = "item_collection_metrics"
    ITEM_COLLECTION_KEY = "item_collection_key"

    ATTRIBUTES_TO_GET = "attributes_to_get"
    CONSISTENT_READ = "consistent_read"
    KEY = "key"
    KEYS = "keys"

    EXCLUSIVE_START_KEY = "exclusive_start_key"
    SCAN_FILTER = "scan_filter"
    SELECT = "select"
    SEGMENT = "segment"
    TOTAL_SEGMENTS = "total_segments"
    ATTRIBUTE_VALUE_LIST = "attribute_value_list"
    COMPARISON_OPERATOR = "comparison_operator"

    KEY_CONDITIONS = "key_conditions"
    SCAN_INDEX_FORWARD = "scan_index_forward"

    COUNT = "count"
    SCANNED_COUNT = "scanned_count"
    ITEMS = "items"
    LAST_EVALUATED_KEY = "last_evaluated_key"

    ATTRIBUTE_UPDATES = "attribute_updates"
    ACTION = "action"

    LINKS = "links"
    HREF = "href"
    REL = "rel"
    SELF = "self"

    REQUEST_ITEMS = "request_items"
    REQUEST_DELETE = "delete_request"
    REQUEST_PUT = "put_request"

    STATUS = "status"
    STRATEGY = "strategy"
    START_DATE_TIME = "start_date_time"
    FINISH_DATE_TIME = "finish_date_time"

    BACKUPS = "backups"
    BACKUP_ID = "backup_id"
    BACKUP_NAME = "backup_name"
    EXCLUSIVE_START_BACKUP_ID = "exclusive_start_backup_id"
    LAST_EVALUATED_BACKUP_ID = "last_evaluated_backup_id"
    LOCATION = "location"

    RESTORE_JOBS = "restore_jobs"
    RESTORE_JOB_ID = "restore_job_id"
    EXCLUSIVE_START_RESTORE_JOB_ID = "exclusive_start_restore_job_id"
    LAST_EVALUATED_RESTORE_JOB_ID = "last_evaluated_restore_job_id"
    SOURCE = "source"


class Values():
    KEY_TYPE_HASH = "HASH"
    KEY_TYPE_RANGE = "RANGE"

    PROJECTION_TYPE_KEYS_ONLY = "KEYS_ONLY"
    PROJECTION_TYPE_INCLUDE = "INCLUDE"
    PROJECTION_TYPE_ALL = "ALL"

    ACTION_TYPE_PUT = models.UpdateItemAction.UPDATE_ACTION_PUT
    ACTION_TYPE_ADD = models.UpdateItemAction.UPDATE_ACTION_ADD
    ACTION_TYPE_DELETE = models.UpdateItemAction.UPDATE_ACTION_DELETE

    TABLE_STATUS_ACTIVE = models.TableMeta.TABLE_STATUS_ACTIVE
    TABLE_STATUS_CREATING = models.TableMeta.TABLE_STATUS_CREATING
    TABLE_STATUS_DELETING = models.TableMeta.TABLE_STATUS_DELETING

    RETURN_VALUES_NONE = models.UpdateReturnValuesType.RETURN_VALUES_TYPE_NONE
    RETURN_VALUES_ALL_OLD = (
        models.UpdateReturnValuesType.RETURN_VALUES_TYPE_ALL_OLD
    )
    RETURN_VALUES_UPDATED_OLD = (
        models.UpdateReturnValuesType.RETURN_VALUES_TYPE_UPDATED_OLD
    )
    RETURN_VALUES_ALL_NEW = (
        models.UpdateReturnValuesType.RETURN_VALUES_TYPE_ALL_NEW
    )
    RETURN_VALUES_UPDATED_NEW = (
        models.UpdateReturnValuesType.RETURN_VALUES_TYPE_UPDATED_NEW
    )

    ALL_ATTRIBUTES = models.SelectType.SELECT_TYPE_ALL
    ALL_PROJECTED_ATTRIBUTES = models.SelectType.SELECT_TYPE_ALL_PROJECTED
    SPECIFIC_ATTRIBUTES = models.SelectType.SELECT_TYPE_SPECIFIC
    COUNT = models.SelectType.SELECT_TYPE_COUNT

    EQ = models.Condition.CONDITION_TYPE_EQUAL

    LE = models.IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL
    LT = models.IndexedCondition.CONDITION_TYPE_LESS
    GE = models.IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL
    GT = models.IndexedCondition.CONDITION_TYPE_GREATER
    BEGINS_WITH = "BEGINS_WITH"
    BETWEEN = "BETWEEN"

    NE = models.ScanCondition.CONDITION_TYPE_NOT_EQUAL
    CONTAINS = models.ScanCondition.CONDITION_TYPE_CONTAINS
    NOT_CONTAINS = models.ScanCondition.CONDITION_TYPE_NOT_CONTAINS
    IN = models.ScanCondition.CONDITION_TYPE_IN

    NOT_NULL = models.ScanCondition.CONDITION_TYPE_NOT_NULL
    NULL = models.ScanCondition.CONDITION_TYPE_NULL

    BOOKMARK = "bookmark"
    SELF = "self"


ATTRIBUTE_NAME_PATTERN = "^\w+"
TABLE_NAME_PATTERN = "^\w+"
INDEX_NAME_PATTERN = "^\w+"


class Parser():
    @classmethod
    def parse_attribute_definition(cls, attr_def_json):
        attr_name_json = attr_def_json.pop(Props.ATTRIBUTE_NAME, None)
        attr_type_json = attr_def_json.pop(Props.ATTRIBUTE_TYPE, None)

        validation.validate_attr_name(attr_name_json)
        storage_type = models.AttributeType(attr_type_json)

        validation.validate_unexpected_props(
            attr_def_json, "attribute_definition"
        )

        return attr_name_json, storage_type

    @classmethod
    def format_attribute_definition(cls, attr_name, attr_type):
        type_json = attr_type.type

        return {
            Props.ATTRIBUTE_NAME: attr_name,
            Props.ATTRIBUTE_TYPE: type_json
        }

    @classmethod
    def parse_attribute_definitions(cls, attr_def_list_json):
        res = {}

        for attr_def_json in attr_def_list_json:
            attr_name, attr_type = (
                cls.parse_attribute_definition(attr_def_json)
            )
            res[attr_name] = attr_type

        return res

    @classmethod
    def format_attribute_definitions(cls, attr_def_map):
        return [
            cls.format_attribute_definition(attr_name, attr_type)
            for attr_name, attr_type in attr_def_map.iteritems()
        ]

    @classmethod
    def parse_key_schema(cls, key_def_list_json):
        hash_key_attr_name = None
        range_key_attr_name = None

        for key_def in key_def_list_json:
            key_attr_name_json = key_def.pop(Props.ATTRIBUTE_NAME, None)
            validation.validate_attr_name(key_attr_name_json)

            key_type_json = key_def.pop(Props.KEY_TYPE, None)

            if key_type_json == Values.KEY_TYPE_HASH:
                if hash_key_attr_name is not None:
                    raise exception.ValidationError(
                        _("Only one 'HASH' key is allowed"))
                hash_key_attr_name = key_attr_name_json
            elif key_type_json == Values.KEY_TYPE_RANGE:
                if range_key_attr_name is not None:
                    raise exception.ValidationError(
                        _("Only one 'RANGE' key is allowed"))
                range_key_attr_name = key_attr_name_json
            else:
                raise exception.ValidationError(
                    _("Only 'RANGE' or 'HASH' key types are allowed, but "
                      "'%(key_type)s' is found"), key_type=key_type_json)

            validation.validate_unexpected_props(key_def, "key_definition")
        if hash_key_attr_name is None:
            raise exception.ValidationError(_("HASH key is missing"))
        if range_key_attr_name:
            return (hash_key_attr_name, range_key_attr_name)
        return (hash_key_attr_name,)

    @classmethod
    def format_key_schema(cls, key_attr_names):
        assert len(key_attr_names) > 0, (
            "At least HASH key should be specified. No one is given"
        )

        assert len(key_attr_names) <= 2, (
            "More then 2 keys given. Only one HASH and one RANGE key allowed"
        )

        res = [
            {
                Props.KEY_TYPE: Values.KEY_TYPE_HASH,
                Props.ATTRIBUTE_NAME: key_attr_names[0]
            }
        ]

        if len(key_attr_names) > 1:
            res.append({
                Props.KEY_TYPE: Values.KEY_TYPE_RANGE,
                Props.ATTRIBUTE_NAME: key_attr_names[1]
            })

        return res

    @classmethod
    def parse_local_secondary_index(cls, local_secondary_index_json):
        key_attrs_json = local_secondary_index_json.pop(Props.KEY_SCHEMA, None)
        validation.validate_list(key_attrs_json, Props.KEY_SCHEMA)
        key_attrs_for_projection = cls.parse_key_schema(key_attrs_json)
        hash_key = key_attrs_for_projection[0]

        try:
            range_key = key_attrs_for_projection[1]
        except IndexError:
            raise exception.ValidationError(
                _("Range key in index wasn't specified"))

        index_name = local_secondary_index_json.pop(Props.INDEX_NAME, None)
        validation.validate_index_name(index_name)

        projection_json = local_secondary_index_json.pop(Props.PROJECTION,
                                                         None)
        validation.validate_object(projection_json, Props.PROJECTION)

        validation.validate_unexpected_props(
            local_secondary_index_json, "local_secondary_index"
        )

        projection_type = projection_json.pop(
            Props.PROJECTION_TYPE, Values.PROJECTION_TYPE_INCLUDE
        )

        if projection_type == Values.PROJECTION_TYPE_ALL:
            projected_attrs = None
        elif projection_type == Values.PROJECTION_TYPE_KEYS_ONLY:
            projected_attrs = tuple()
        elif projection_type == Values.PROJECTION_TYPE_INCLUDE:
            projected_attrs = projection_json.pop(
                Props.NON_KEY_ATTRIBUTES, None
            )
        else:
            raise exception.ValidationError(
                _("Only '%(pt_all)', '%(pt_ko)' of '%(pt_incl)' projection "
                  "types are allowed, but '%(projection_type)s' is found"),
                pt_all=Values.PROJECTION_TYPE_ALL,
                pt_ko=Values.PROJECTION_TYPE_KEYS_ONLY,
                pt_incl=Values.PROJECTION_TYPE_INCLUDE,
                projection_type=projection_type
            )
        validation.validate_unexpected_props(projection_json, Props.PROJECTION)

        return index_name, models.IndexDefinition(
            hash_key,
            range_key,
            projected_attrs
        )

    @classmethod
    def format_local_secondary_index(cls, index_name, hash_key,
                                     local_secondary_index):
        if local_secondary_index.projected_attributes:
            projection = {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_INCLUDE,
                Props.NON_KEY_ATTRIBUTES: list(
                    local_secondary_index.projected_attributes
                )
            }
        elif local_secondary_index.projected_attributes is None:
            projection = {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_ALL
            }
        else:
            projection = {
                Props.PROJECTION_TYPE: Values.PROJECTION_TYPE_KEYS_ONLY
            }

        return {
            Props.INDEX_NAME: index_name,
            Props.KEY_SCHEMA: cls.format_key_schema(
                (local_secondary_index.alt_hash_key_attr,
                 local_secondary_index.alt_range_key_attr)
            ),
            Props.PROJECTION: projection,
            Props.ITEM_COUNT: 0,
            Props.INDEX_SIZE_BYTES: 0
        }

    @classmethod
    def parse_local_secondary_indexes(cls, local_secondary_index_list_json):
        res = {}
        for index_json in local_secondary_index_list_json:
            index_name, index_def = (
                cls.parse_local_secondary_index(index_json)
            )
            res[index_name] = index_def

        if len(res) < len(local_secondary_index_list_json):
            raise exception.ValidationError(
                _("Two or more indexes with the same name"))

        return res

    @classmethod
    def format_local_secondary_indexes(cls, hash_key,
                                       local_secondary_index_map):
        return [
            cls.format_local_secondary_index(index_name, hash_key, index_def)
            for index_name, index_def in local_secondary_index_map.iteritems()
        ]

    @classmethod
    def encode_attr_value(cls, attr_value):
        return {
            attr_value.attr_type.type: attr_value.encoded_value
        }

    @classmethod
    def parse_typed_attr_value(cls, typed_attr_value_json):
        if len(typed_attr_value_json) != 1:
            raise exception.ValidationError(
                _("Can't recognize attribute typed value format: '%(attr)s'"),
                attr=json.dumps(typed_attr_value_json)
            )
        (attr_type_json, attr_value_json) = (
            typed_attr_value_json.popitem()
        )

        return models.AttributeValue(attr_type_json, attr_value_json)

    @classmethod
    def parse_item_attributes(cls, item_attributes_json):
        item = {}
        for (attr_name_json, typed_attr_value_json) in (
                item_attributes_json.iteritems()):
            validation.validate_attr_name(attr_name_json)
            validation.validate_object(typed_attr_value_json, attr_name_json)
            item[attr_name_json] = cls.parse_typed_attr_value(
                typed_attr_value_json
            )

        return item

    @classmethod
    def format_item_attributes(cls, item_attributes):
        attributes_json = {}
        for (attr_name, attr_value) in item_attributes.iteritems():
            attributes_json[attr_name] = cls.encode_attr_value(attr_value)

        return attributes_json

    @classmethod
    def parse_expected_attribute_conditions(
            cls, expected_attribute_conditions_json):
        expected_attribute_conditions = {}

        for (attr_name_json, condition_json) in (
                expected_attribute_conditions_json.iteritems()):
            validation.validate_attr_name(attr_name_json)
            validation.validate_object(condition_json, attr_name_json)

            if len(condition_json) != 1:
                raise exception.ValidationError(
                    _("Can't recognize attribute expected condition format: "
                      "'%(attr)s'"),
                    attr=json.dumps(condition_json)
                )

            (condition_type, condition_value) = condition_json.popitem()

            validation.validate_string(condition_type, "condition type")

            if condition_type == Props.VALUE:
                validation.validate_object(condition_value, Props.VALUE)
                expected_attribute_conditions[attr_name_json] = [
                    models.ExpectedCondition.eq(
                        cls.parse_typed_attr_value(condition_value)
                    )
                ]
            elif condition_type == Props.EXISTS:
                validation.validate_boolean(condition_value, Props.EXISTS)
                expected_attribute_conditions[attr_name_json] = [
                    models.ExpectedCondition.not_null() if condition_value else
                    models.ExpectedCondition.null()
                ]
            else:
                raise exception.ValidationError(
                    _("Unsupported condition type found: %(condition_type)s"),
                    condition_type=condition_type
                )

        return expected_attribute_conditions

    @classmethod
    def parse_select_type(cls, select, attributes_to_get,
                          select_on_index=False):
        if select is None:
            if attributes_to_get:
                return models.SelectType.specific_attributes(
                    attributes_to_get
                )
            else:
                if select_on_index:
                    return models.SelectType.all_projected()
                else:
                    return models.SelectType.all()

        if select == Values.SPECIFIC_ATTRIBUTES:
            assert attributes_to_get
            return models.SelectType.specific_attributes(attributes_to_get)

        assert not attributes_to_get

        if select == Values.ALL_ATTRIBUTES:
            return models.SelectType.all()

        if select == Values.ALL_PROJECTED_ATTRIBUTES:
            assert select_on_index
            return models.SelectType.all_projected()

        if select == Values.COUNT:
            return models.SelectType.count()

        assert False, "Select type wasn't recognized"

    SINGLE_ARGUMENT_CONDITIONS = {
        Values.EQ, Values.GT, Values.GE, Values.LT, Values.LE,
        Values.BEGINS_WITH, Values.NE, Values.CONTAINS, Values.NOT_CONTAINS
    }

    @classmethod
    def parse_attribute_condition(cls, condition_type, condition_args,
                                  condition_class=models.IndexedCondition):

        actual_args_count = (
            len(condition_args) if condition_args is not None else 0
        )
        if condition_type == Values.BETWEEN:
            if actual_args_count != 2:
                raise exception.ValidationError(
                    _("%(type)s condition type requires exactly 2 arguments, "
                      "but %(actual_args_count)s given"),
                    type=condition_type,
                    actual_args_count=actual_args_count
                )
            if condition_args[0].attr_type != condition_args[1].attr_type:
                raise exception.ValidationError(
                    _("%(type)s condition type requires arguments of the "
                      "same type, but different types given"),
                    type=condition_type,
                )

            return [
                condition_class.ge(condition_args[0]),
                condition_class.le(condition_args[1])
            ]

        if condition_type == Values.BEGINS_WITH:
            first_condition = condition_class(
                condition_class.CONDITION_TYPE_GREATER_OR_EQUAL,
                condition_args
            )
            condition_arg = first_condition.arg

            if condition_arg.is_number:
                raise exception.ValidationError(
                    _("%(condition_type)s condition type is not allowed for"
                      "argument of the %(argument_type)s type"),
                    condition_type=condition_type,
                    argument_type=condition_arg.attr_type.type
                )

            first_value = condition_arg.decoded_value
            chr_fun = unichr if isinstance(first_value, unicode) else chr
            second_value = first_value[:-1] + chr_fun(ord(first_value[-1]) + 1)

            second_condition = condition_class.le(
                models.AttributeValue(
                    condition_arg.attr_type, decoded_value=second_value
                )
            )

            return [first_condition, second_condition]

        return [condition_class(condition_type, condition_args)]

    @classmethod
    def parse_attribute_conditions(cls, attribute_conditions_json,
                                   condition_class=models.IndexedCondition):
        attribute_conditions = {}

        for (attr_name, condition_json) in (
                attribute_conditions_json.iteritems()):
            validation.validate_attr_name(attr_name)
            validation.validate_object(condition_json, attr_name)

            condition_type_json = (
                condition_json.pop(Props.COMPARISON_OPERATOR, None)
            )

            attribute_list = condition_json.pop(Props.ATTRIBUTE_VALUE_LIST,
                                                None)
            condition_args = []
            if attribute_list:
                validation.validate_list_of_objects(
                    attribute_list, Props.ATTRIBUTE_VALUE_LIST
                )
                for typed_attribute_value in attribute_list:
                    condition_args.append(
                        cls.parse_typed_attr_value(typed_attribute_value)
                    )

            attribute_conditions[attr_name] = (
                cls.parse_attribute_condition(
                    condition_type_json, condition_args, condition_class
                )
            )
            validation.validate_unexpected_props(condition_json, attr_name)

        return attribute_conditions

    @classmethod
    def parse_attribute_updates(cls, attribute_updates_json):
        attribute_updates = {}

        for attr, attr_update_json in attribute_updates_json.iteritems():
            validation.validate_attr_name(attr)
            validation.validate_object(attr_update_json, attr)

            action_type_json = attr_update_json.pop(Props.ACTION, None)
            validation.validate_string(action_type_json, Props.ACTION)

            value_json = attr_update_json.pop(Props.VALUE, None)

            if value_json:
                validation.validate_object(value_json, Props.VALUE)
                value = cls.parse_typed_attr_value(value_json)
            else:
                value = None

            update_action = models.UpdateItemAction(action_type_json, value)

            validation.validate_unexpected_props(attr_update_json, attr)

            attribute_updates[attr] = update_action

        return attribute_updates

    @classmethod
    def parse_batch_write_request_items(cls, request_items_json):
        request_map = {}
        for table_name, request_list_json in request_items_json.iteritems():
            validation.validate_table_name(table_name)
            validation.validate_list_of_objects(request_list_json, table_name)

            request_list_for_table = []
            for request_json in request_list_json:
                for request_type, request_body in request_json.iteritems():
                    validation.validate_string(request_type, "request_type")
                    if request_type == Props.REQUEST_PUT:
                        validation.validate_object(request_body, request_type)
                        item = request_body.pop(Props.ITEM, None)
                        validation.validate_object(item, Props.ITEM)
                        validation.validate_unexpected_props(request_body,
                                                             request_type)
                        request_list_for_table.append(
                            models.WriteItemRequest.put(
                                cls.parse_item_attributes(item)
                            )
                        )
                    elif request_type == Props.REQUEST_DELETE:
                        validation.validate_object(request_body, request_type)
                        key = request_body.pop(Props.KEY, None)
                        validation.validate_object(key, Props.KEY)
                        validation.validate_unexpected_props(request_body,
                                                             request_type)
                        request_list_for_table.append(
                            models.WriteItemRequest.delete(
                                cls.parse_item_attributes(key)
                            )
                        )
                    else:
                        raise exception.ValidationError(
                            _("Unsupported request type found: "
                              "%(request_type)s"),
                            request_type=request_type
                        )
            request_map[table_name] = request_list_for_table
        return request_map

    @classmethod
    def parse_batch_get_request_items(cls, request_items_json):
        request_list = []
        for table_name, request_body in request_items_json.iteritems():
            validation.validate_table_name(table_name)
            validation.validate_object(request_body, table_name)

            consistent = request_body.pop(Props.CONSISTENT_READ, False)

            validation.validate_boolean(consistent, Props.CONSISTENT_READ)

            attributes_to_get = request_body.pop(
                Props.ATTRIBUTES_TO_GET, None
            )

            if attributes_to_get is not None:
                attributes_to_get = validation.validate_set(
                    attributes_to_get, Props.ATTRIBUTES_TO_GET
                )
                for attr_name in attributes_to_get:
                    validation.validate_attr_name(attr_name)

            keys = request_body.pop(Props.KEYS, None)

            validation.validate_list(keys, Props.KEYS)

            validation.validate_unexpected_props(request_body, table_name)

            for key in keys:
                key_attribute_map = cls.parse_item_attributes(key)
                request_list.append(
                    models.GetItemRequest(
                        table_name, key_attribute_map, attributes_to_get,
                        consistent=consistent
                    )
                )
        return request_list

    @classmethod
    def format_request_items(cls, request_items):
        res = {}
        for table_name, request_list in request_items.iteritems():
            table_requests = []
            for request in request_list:
                if request.is_put:
                    request_json = {
                        Props.REQUEST_PUT: {
                            Props.ITEM: cls.format_item_attributes(
                                request.attribute_map)
                        }

                    }
                elif request.is_delete:
                    request_json = {
                        Props.REQUEST_DELETE: {
                            Props.KEY: cls.format_item_attributes(
                                request.attribute_map)
                        }
                    }
                else:
                    assert False, (
                        "Unknown request type '{}'".format(
                            request.type
                        )
                    )

                table_requests.append(request_json)
            res[table_name] = table_requests

        return res

    @classmethod
    def format_batch_get_unprocessed(cls, unprocessed, request_items):
        res = {}
        for request in unprocessed:
            tname = request.table_name
            table_res = res.get(request.table_name, None)
            if table_res is None:
                table_res = {Props.KEYS: []}
                res[tname] = table_res
            table_res[Props.KEYS].append(
                cls.format_item_attributes(request.key_attribute_map)
            )
            attr_to_get = request_items[tname].get(Props.ATTRIBUTES_TO_GET)
            consistent = request_items[tname].get(Props.CONSISTENT_READ)
            if attr_to_get:
                table_res[Props.ATTRIBUTES_TO_GET] = attr_to_get
            if consistent:
                table_res[Props.CONSISTENT_READ] = consistent
        return res

    @classmethod
    def format_table_status(cls, table_status):
        return table_status

    @classmethod
    def format_backup(cls, backup, self_link_prefix):
        if not backup:
            return {}

        res = {
            Props.BACKUP_ID: backup.id.hex,
            Props.BACKUP_NAME: backup.name,
            Props.TABLE_NAME: backup.table_name,
            Props.STATUS: backup.status,
            Props.STRATEGY: backup.strategy,
            Props.START_DATE_TIME: backup.start_date_time,
            Props.LOCATION: backup.location
        }

        if backup.finish_date_time:
            res[Props.FINISH_DATE_TIME] = backup.finish_date_time

        links = [
            {
                Props.REL: Props.SELF,
                Props.HREF: cls.format_backup_href(backup, self_link_prefix)
            },
            {
                Props.REL: Props.LOCATION,
                Props.HREF: backup.location
            }
        ]

        res[Props.LINKS] = links

        return res

    @classmethod
    def format_backup_href(cls, backup, self_link_prefix):
        return '{}/{}'.format(self_link_prefix, backup.id.hex)

    @classmethod
    def format_restore_job(cls, restore_job, self_link_prefix):
        if not restore_job:
            return {}

        res = {
            Props.RESTORE_JOB_ID: restore_job.id.hex,
            Props.TABLE_NAME: restore_job.table_name,
            Props.STATUS: restore_job.status,
            Props.START_DATE_TIME: restore_job.start_date_time,
        }

        if restore_job.backup_id:
            res[Props.BACKUP_ID] = restore_job.backup_id.hex

        if restore_job.source:
            res[Props.SOURCE] = restore_job.source

        if restore_job.finish_date_time:
            res[Props.FINISH_DATE_TIME] = restore_job.finish_date_time

        links = [
            {
                Props.REL: Props.SELF,
                Props.HREF: cls.format_restore_job_href(
                    restore_job, self_link_prefix)
            },
            {
                Props.REL: Props.SOURCE,
                Props.HREF: restore_job.source
            }
        ]

        res[Props.LINKS] = links

        return res

    @classmethod
    def format_restore_job_href(cls, restore_job, self_link_prefix):
        return '{}/{}'.format(self_link_prefix, restore_job.id.hex)
