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
import decimal
import json


class ModelBase(object):

    def __repr__(self):
        return self.to_json()

    def __init__(self, **kwargs):
        self.__dict__["_data"] = kwargs
        self.__dict__["_hash"] = None

    def __setattr__(self, key, value):
        if key == "_hash":
            self.__dict__[key] = value
        else:
            raise AttributeError("Object is read only")

    def __getattr__(self, key):
        return self._data[key]

    def __getitem__(self, key):
        return self._data[key]

    def __eq__(self, other):
        return (
            type(other) == type(self) and
            cmp(self._data, other._data) == 0
        )

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        if not self._hash:
            self._hash = hash(tuple(sorted(self._data.items())))

        return self._hash

    def to_json(self):
        def encode_model(obj):
            if isinstance(obj, ModelBase):
                data = obj._data.copy()
                data["__model__"] = obj.__class__.__name__
                return data
            raise TypeError(repr(obj) + " is not JSON serializable")

        return json.dumps(self, default=encode_model, sort_keys=True)

    @staticmethod
    def from_json(jsn):
        def as_model(dct):
            model_cls = dct.pop('__model__', None)
            if model_cls:
                return eval(model_cls)(**dct)
            return dct

        return json.loads(jsn, object_hook=as_model)


class AttributeType(ModelBase):
    ELEMENT_TYPE_STRING = "s"
    ELEMENT_TYPE_NUMBER = "n"
    ELEMENT_TYPE_BLOB = "b"

    COLLECTION_TYPE_SET = "set"

    _allowed_types = set([ELEMENT_TYPE_STRING, ELEMENT_TYPE_NUMBER,
                          ELEMENT_TYPE_BLOB])

    _allowed_collection_types = set([None, COLLECTION_TYPE_SET])

    def __init__(self, element_type, collection_type=None):
        assert element_type in self._allowed_types, (
            "Attribute type '%s' isn't allowed" % element_type
        )

        assert collection_type in self._allowed_collection_types, (
            "Attribute type collection '%s' isn't allowed" % collection_type
        )

        super(AttributeType, self).__init__(element_type=element_type,
                                            collection_type=collection_type)


ATTRIBUTE_TYPE_STRING = AttributeType(AttributeType.ELEMENT_TYPE_STRING)
ATTRIBUTE_TYPE_STRING_SET = AttributeType(AttributeType.ELEMENT_TYPE_STRING,
                                          AttributeType.COLLECTION_TYPE_SET)
ATTRIBUTE_TYPE_NUMBER = AttributeType(AttributeType.ELEMENT_TYPE_NUMBER)
ATTRIBUTE_TYPE_NUMBER_SET = AttributeType(AttributeType.ELEMENT_TYPE_NUMBER,
                                          AttributeType.COLLECTION_TYPE_SET)
ATTRIBUTE_TYPE_BLOB = AttributeType(AttributeType.ELEMENT_TYPE_BLOB)
ATTRIBUTE_TYPE_BLOB_SET = AttributeType(AttributeType.ELEMENT_TYPE_BLOB,
                                        AttributeType.COLLECTION_TYPE_SET)

ORDER_TYPE_ASC = "ASC"
ORDER_TYPE_DESC = "DESC"


class AttributeValue(ModelBase):
    def __init__(self, attr_type, attr_value):
        if attr_type == ATTRIBUTE_TYPE_STRING:
            value = self.__create_str(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_NUMBER:
            value = self.__create_number(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_BLOB:
            value = self.__create_blob(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_STRING_SET:
            value = self.__create_str_set(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_NUMBER_SET:
            value = self.__create_number_set(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_BLOB_SET:
            value = self.__create_blob_set(attr_value)
        else:
            assert False, "Attribute type wasn't recognized"

        super(AttributeValue, self).__init__(type=attr_type, value=value)

    @property
    def is_str(self):
        return self.type == ATTRIBUTE_TYPE_STRING

    @property
    def is_number(self):
        return self.type == ATTRIBUTE_TYPE_NUMBER

    @property
    def is_str_set(self):
        return self.type == ATTRIBUTE_TYPE_STRING_SET

    @property
    def is_number_set(self):
        return self.type == ATTRIBUTE_TYPE_NUMBER_SET

    @property
    def is_blob_set(self):
        return self.type == ATTRIBUTE_TYPE_BLOB_SET

    @classmethod
    def __create_str(cls, str_value):
        assert isinstance(str_value, (str, unicode))
        return str_value

    @classmethod
    def __create_number(cls, number_value):
        return decimal.Decimal(number_value)

    @classmethod
    def __create_blob(cls, blob_value):
        assert isinstance(blob_value, (str, unicode))
        return blob_value

    @classmethod
    def __create_str_set(cls, str_set_value):
        return frozenset(map(cls.__create_str, str_set_value))

    @classmethod
    def __create_number_set(cls, number_set_value):
        return frozenset(map(cls.__create_number, number_set_value))

    @classmethod
    def __create_blob_set(cls, blob_set_value):
        return frozenset(map(cls.__create_blob, blob_set_value))

    @classmethod
    def str(cls, str_value):
        assert isinstance(str_value, (str, unicode))
        return cls(ATTRIBUTE_TYPE_STRING, str_value)

    @classmethod
    def blob(cls, blob_value):
        return cls(ATTRIBUTE_TYPE_BLOB, blob_value)

    @classmethod
    def number(cls, number_value):
        return cls(ATTRIBUTE_TYPE_NUMBER, number_value)

    @classmethod
    def str_set(cls, string_set_value):
        return cls(ATTRIBUTE_TYPE_STRING_SET, string_set_value)

    @classmethod
    def blob_set(cls, blob_set_value):
        return cls(ATTRIBUTE_TYPE_BLOB_SET, blob_set_value)

    @classmethod
    def number_set(cls, number_set_value):
        return cls(ATTRIBUTE_TYPE_NUMBER_SET, number_set_value)


class Condition(ModelBase):
    CONDITION_TYPE_EQUAL = "e"

    _allowed_types = set([CONDITION_TYPE_EQUAL])

    def __init__(self, condition_type, condition_arg):
        assert condition_type in self._allowed_types, (
            "Condition type '%s' isn't allowed" % condition_type
        )
        super(Condition, self).__init__(type=condition_type, arg=condition_arg)

    @classmethod
    def eq(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_EQUAL, condition_arg)


class IndexedCondition(Condition):
    CONDITION_TYPE_LESS = "l"
    CONDITION_TYPE_LESS_OR_EQUAL = "le"
    CONDITION_TYPE_GREATER = "g"
    CONDITION_TYPE_GREATER_OR_EQUAL = "ge"

    _allowed_types = set([Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_LESS,
                          CONDITION_TYPE_LESS_OR_EQUAL, CONDITION_TYPE_GREATER,
                          CONDITION_TYPE_GREATER_OR_EQUAL])

    @classmethod
    def lt(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_LESS, condition_arg)

    @classmethod
    def le(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_LESS_OR_EQUAL, condition_arg)

    @classmethod
    def gt(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_GREATER, condition_arg)

    @classmethod
    def ge(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_GREATER_OR_EQUAL, condition_arg)

    def is_strict_border(self):
        return self.type not in (self.CONDITION_TYPE_LESS_OR_EQUAL,
                                 self.CONDITION_TYPE_GREATER_OR_EQUAL)

    def is_right_border(self):
        return self.type in (self.CONDITION_TYPE_LESS,
                             self.CONDITION_TYPE_LESS_OR_EQUAL)

    def is_left_border(self):
        return self.type in (self.CONDITION_TYPE_GREATER,
                             self.CONDITION_TYPE_GREATER_OR_EQUAL)


class ScanCondition(IndexedCondition):

    CONDITION_TYPE_IN = "in"
    CONDITION_TYPE_CONTAINS = "contains"
    CONDITION_TYPE_NOT_CONTAINS = "not_contains"
    CONDITION_TYPE_NOT_EQUAL = "ne"

    _allowed_types = set([Condition.CONDITION_TYPE_EQUAL,
                          IndexedCondition.CONDITION_TYPE_LESS,
                          IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL,
                          IndexedCondition.CONDITION_TYPE_GREATER,
                          IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL,
                          CONDITION_TYPE_IN, CONDITION_TYPE_CONTAINS,
                          CONDITION_TYPE_NOT_CONTAINS,
                          CONDITION_TYPE_NOT_EQUAL])

    @classmethod
    def neq(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_NOT_EQUAL, condition_arg)

    @classmethod
    def in_set(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_IN, condition_arg)

    @classmethod
    def contains(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_CONTAINS, condition_arg)

    @classmethod
    def not_contains(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_NOT_CONTAINS, condition_arg)


class ExpectedCondition(Condition):
    CONDITION_TYPE_EXISTS = "exists"

    _allowed_types = set(
        [Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_EXISTS])

    @classmethod
    def exists(cls):
        return cls(cls.CONDITION_TYPE_EXISTS, True)

    @classmethod
    def not_exists(cls):
        return cls(cls.CONDITION_TYPE_EXISTS, False)


class SelectType(ModelBase):
    SELECT_TYPE_ALL = "all"
    SELECT_TYPE_ALL_PROJECTED = "all_projected"
    SELECT_TYPE_SPECIFIED = "specified"
    SELECT_TYPE_COUNT = "count"

    _allowed_types = set([SELECT_TYPE_ALL, SELECT_TYPE_ALL_PROJECTED,
                          SELECT_TYPE_SPECIFIED, SELECT_TYPE_COUNT])

    def __init__(self, select_type, attributes=None):
        assert select_type in self._allowed_types, (
            "Select type '%s' isn't allowed" % select_type
        )
        super(SelectType, self).__init__(type=select_type,
                                         attributes=attributes)

    @classmethod
    def all(cls):
        return cls(cls.SELECT_TYPE_ALL)

    @classmethod
    def all_projected(cls):
        return cls(cls.SELECT_TYPE_ALL_PROJECTED)

    @classmethod
    def count(cls):
        return cls(cls.SELECT_TYPE_COUNT)

    @classmethod
    def specified_attributes(cls, attributes):
        return cls(cls.SELECT_TYPE_ALL_PROJECTED, frozenset(attributes))

    @property
    def is_count(self):
        return self.type == self.SELECT_TYPE_COUNT

    @property
    def is_all(self):
        return self.type == self.SELECT_TYPE_ALL

    @property
    def is_all_projected(self):
        return self.type == self.SELECT_TYPE_ALL_PROJECTED

    @property
    def is_specified(self):
        return self.type == self.SELECT_TYPE_SPECIFIED


class WriteItemBatchableRequest(ModelBase):
    def __init__(self, table_name, **kwargs):
        """
        @param table_name: String, name of table to delete item from
        @param timestamp: timestamp of operation. Operation will be skipped
                    if another one already performed with greater or equal
                    timestamp
        """
        super(WriteItemBatchableRequest, self).__init__(table_name=table_name,
                                                        **kwargs)


class DeleteItemRequest(WriteItemBatchableRequest):
    def __init__(self, table_name, key_attribute_map):
        """
        @param table_name: String, name of table to delete item from
        @param key_attribute_map: key attribute name to
                    AttributeValue mapping. It defines row to be deleted
        @param indexed_condition_map: indexed attribute name to
                    IndexedCondition instance mapping. It defines rows
                    set to be removed
        """
        super(DeleteItemRequest, self).__init__(
            table_name, key_attribute_map=key_attribute_map)


class PutItemRequest(WriteItemBatchableRequest):
    def __init__(self, table_name, attribute_map):
        """
        @param table_name: String, name of table to delete item from
        @param attribute_map: attribute name to AttributeValue mapping.
                    It defines row key and additional attributes to put
                    item
        """
        super(PutItemRequest, self).__init__(
            table_name, attribute_map=attribute_map)


class UpdateItemAction(ModelBase):
    UPDATE_ACTION_PUT = "put"
    UPDATE_ACTION_DELETE = "del"
    UPDATE_ACTION_ADD = "add"

    _allowed_actions = set([UPDATE_ACTION_PUT, UPDATE_ACTION_DELETE,
                            UPDATE_ACTION_ADD])

    def __init__(self, action, value):
        """
        @param action: one of available action names
        @param value: AttributeValue instance, parameter for action
        """
        assert action in self._allowed_actions, (
            "Update action '%s' isn't allowed" % action
        )

        super(UpdateItemAction, self).__init__(action=action, value=value)


class IndexDefinition(ModelBase):
    def __init__(self, attribute_to_index, projected_attributes=None):
        """
        @param index_name: name of index
        @param attribute_to_index: attribute name to be indexed
        @param projected_attributes: set of non key attribute names to be
                    projected. If 'None' - all attributes will be projected
        """
        projected_attributes = (
            None if projected_attributes is None else
            frozenset(projected_attributes)
        )

        super(IndexDefinition, self).__init__(
            attribute_to_index=attribute_to_index,
            projected_attributes=projected_attributes
        )


class SelectResult(ModelBase):

    def __init__(self, items=None, last_evaluated_key=None, count=None,
                 **kwargs):
        """
        @param items: list of attribute name to AttributeValue mappings
        @param last_evaluated_key: attribute name to AttributeValue mapping,
                    which defines last evaluated key
        """

        if count is None:
            assert items is not None
            count = len(items)
        else:
            assert (items is None) or (count == len(items))

        super(SelectResult, self).__init__(
            items=items, count=count, last_evaluated_key=last_evaluated_key,
            **kwargs)


class ScanResult(SelectResult):

    def __init__(self, items=None, last_evaluated_key=None,
                 count=None, scanned_count=None):

        super(ScanResult, self).__init__(items, last_evaluated_key, count,
                                         scanned_count=scanned_count)


class TableSchema(ModelBase):

    def __init__(self, attribute_type_map, key_attributes, index_def_map=None):
        """
        @param attribute_type_map: attribute name to AttributeType mapping
        @param key_attrs: list of key attribute names, contains partition key
                    (the first in list, required) attribute name and extra key
                    attribute names (the second and other list items, not
                    required)
        @param index_def_map: index name to IndexDefinition mapping
        """

        if index_def_map is None:
            index_def_map = {}

        super(TableSchema, self).__init__(
            attribute_type_map=attribute_type_map,
            key_attributes=key_attributes,
            index_def_map=index_def_map)


class TableMeta(ModelBase):
    TABLE_STATUS_CREATING = "creating"
    TABLE_STATUS_DELETING = "deleting"
    TABLE_STATUS_ACTIVE = "active"

    _allowed_statuses = set([TABLE_STATUS_CREATING, TABLE_STATUS_DELETING,
                             TABLE_STATUS_ACTIVE])

    def __init__(self, schema, status):
        """
        @param table_schema: TableSchema instance
        @param status: table status
        """

        assert status in self._allowed_statuses, (
            "Table status '%s' isn't allowed" % status
        )

        super(TableMeta, self).__init__(schema=schema, status=status)
