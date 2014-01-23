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


class ModelBase(object):

    _data_fields = []

    def __init__(self):
        self.__hash = None

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __eq__(self, other):
        for field in self._data_fields:
            if self[field] != other[field]:
                return False

        return True

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        if not self.__hash:
            fields_as_list = []
            for field in self._data_fields:
                fields_as_list.append(self[field])
                self.__hash = hash(tuple(fields_as_list))

        return self.__hash


class AttributeType(ModelBase):
    ELEMENT_TYPE_STRING = "string"
    ELEMENT_TYPE_NUMBER = "number"
    ELEMENT_TYPE_BLOB = "blob"

    COLLECTION_TYPE_SET = "set"

    _allowed_types = {ELEMENT_TYPE_STRING, ELEMENT_TYPE_NUMBER,
                      ELEMENT_TYPE_BLOB}

    _allowed_collection_types = {None, COLLECTION_TYPE_SET}

    _data_fields = ['element_type', 'collection_type']

    def __init__(self, element_type, collection_type=None):
        super(AttributeType, self).__init__()

        assert element_type in self._allowed_types, (
            "Attribute type '%s' isn't allowed" % element_type
        )

        assert collection_type in self._allowed_collection_types, (
            "Attribute type collection '%s' isn't allowed" % collection_type
        )

        self._element_type = element_type
        self._collection_type = collection_type

    @property
    def element_type(self):
        return self._element_type

    @property
    def collection_type(self):
        return self._collection_type


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


class AttributeDefinition(ModelBase):

    _data_fields = ['name', 'type']

    def __init__(self, attr_name, attr_type):
        super(AttributeDefinition, self).__init__()

        self._name = attr_name
        self._type = attr_type

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type


class AttributeValue(ModelBase):

    _data_fields = ['value', 'type']

    def __init__(self, attr_type, attr_value):
        super(AttributeValue, self).__init__()

        self._type = attr_type

        if attr_type == ATTRIBUTE_TYPE_STRING:
            self._value = self.__create_str(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_NUMBER:
            self._value = self.__create_number(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_BLOB:
            self._value = self.__create_blob(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_STRING_SET:
            self._value = self.__create_str_set(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_NUMBER_SET:
            self._value = self.__create_number_set(attr_value)
        elif attr_type == ATTRIBUTE_TYPE_BLOB_SET:
            self._value = self.__create_blob_set(attr_value)
        else:
            assert False, "Attribute type wasn't recognized"

    @property
    def value(self):
        return self._value

    @property
    def type(self):
        return self._type

    @property
    def is_str(self):
        return self._type == ATTRIBUTE_TYPE_STRING

    @property
    def is_number(self):
        return self._type == ATTRIBUTE_TYPE_NUMBER

    @property
    def is_str_set(self):
        return self._type == ATTRIBUTE_TYPE_STRING_SET

    @property
    def is_number_set(self):
        return self._type == ATTRIBUTE_TYPE_NUMBER_SET

    @property
    def is_blob_set(self):
        return self._type == ATTRIBUTE_TYPE_BLOB_SET

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


class Condition(object):
    CONDITION_TYPE_EQUAL = "equal"

    _allowed_types = {CONDITION_TYPE_EQUAL}

    def __init__(self, condition_type, condition_arg):
        assert condition_type in self._allowed_types, (
            "Condition type '%s' isn't allowed" % condition_type
        )

        self._condition_type = condition_type
        self._condition_arg = condition_arg

    @property
    def type(self):
        return self._condition_type

    @property
    def arg(self):
        return self._condition_arg

    @classmethod
    def eq(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_EQUAL, condition_arg)


class IndexedCondition(Condition):
    CONDITION_TYPE_LESS = "less"
    CONDITION_TYPE_LESS_OR_EQUAL = "less_or_equal"
    CONDITION_TYPE_GREATER = "greater"
    CONDITION_TYPE_GREATER_OR_EQUAL = "greater_or_equal"

    CONDITION_TYPE_BETWEEN = "between"
    CONDITION_TYPE_BEGINS_WITH = "begins_with"

    _allowed_types = {Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_LESS,
                      CONDITION_TYPE_LESS_OR_EQUAL, CONDITION_TYPE_GREATER,
                      CONDITION_TYPE_GREATER_OR_EQUAL, CONDITION_TYPE_BETWEEN,
                      CONDITION_TYPE_BEGINS_WITH}

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

    @classmethod
    def btw(cls, condition_arg1, condition_arg2):
        return cls(cls.CONDITION_TYPE_BETWEEN,
                   (condition_arg1, condition_arg2))

    @classmethod
    def begins_with(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_BEGINS_WITH, condition_arg)


class ScanCondition(IndexedCondition):

    CONDITION_TYPE_IN = "in"
    CONDITION_TYPE_CONTAINS = "contains"
    CONDITION_TYPE_NOT_CONTAINS = "not_contains"
    CONDITION_TYPE_NOT_EQUAL = "not_equal"

    _allowed_types = {Condition.CONDITION_TYPE_EQUAL,
                      IndexedCondition.CONDITION_TYPE_LESS,
                      IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL,
                      IndexedCondition.CONDITION_TYPE_GREATER,
                      IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL,
                      IndexedCondition.CONDITION_TYPE_BETWEEN,
                      IndexedCondition.CONDITION_TYPE_BEGINS_WITH,
                      CONDITION_TYPE_IN, CONDITION_TYPE_CONTAINS,
                      CONDITION_TYPE_NOT_CONTAINS, CONDITION_TYPE_NOT_EQUAL}

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

    _allowed_types = {Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_EXISTS}

    @classmethod
    def exists(cls):
        return cls(cls.CONDITION_TYPE_EXISTS, True)

    @classmethod
    def not_exists(cls):
        return cls(cls.CONDITION_TYPE_EXISTS, False)


class SelectType(object):
    SELECT_TYPE_ALL = "all"
    SELECT_TYPE_ALL_PROJECTED = "all_projected"
    SELECT_TYPE_SPECIFIED = "specified"
    SELECT_TYPE_COUNT = "count"

    _allowed_types = {SELECT_TYPE_ALL, SELECT_TYPE_ALL_PROJECTED,
                      SELECT_TYPE_SPECIFIED, SELECT_TYPE_COUNT}

    def __init__(self, select_type, attributes=None):
        assert select_type in self._allowed_types, (
            "Select type '%s' isn't allowed" % select_type
        )

        self._select_type = select_type
        self._attributes = attributes

    @property
    def type(self):
        return self._select_type

    @property
    def attributes(self):
        return self._attributes

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
        return self._select_type == self.SELECT_TYPE_COUNT

    @property
    def is_all(self):
        return self._select_type == self.SELECT_TYPE_ALL

    @property
    def is_all_projected(self):
        return self._select_type == self.SELECT_TYPE_ALL_PROJECTED

    @property
    def is_specified(self):
        return self._select_type == self.SELECT_TYPE_SPECIFIED


class WriteItemBatchableRequest(object):
    def __init__(self, table_name, timestamp=None):
        """
        @param table_name: String, name of table to delete item from
        @param timestamp: timestamp of operation. Operation will be skipped
                    if another one already performed with greater or equal
                    timestamp
        """
        self._table_name = table_name
        self._timestamp = timestamp

    @property
    def table_name(self):
        return self._table_name

    @property
    def timestamp(self):
        return self._timestamp


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
        super(DeleteItemRequest, self).__init__(table_name)

        self._key_attribute_map = key_attribute_map

    @property
    def key_attribute_map(self):
        return self._key_attribute_map


class PutItemRequest(WriteItemBatchableRequest):
    def __init__(self, table_name, attribute_map):
        """
        @param table_name: String, name of table to delete item from
        @param attribute_map: attribute name to AttributeValue mapping.
                    It defines row key and additional attributes to put
                    item
        """
        super(PutItemRequest, self).__init__(table_name)

        self._attribute_map = attribute_map

    @property
    def attribute_map(self):
        return self._attribute_map


class UpdateItemAction(object):
    UPDATE_ACTION_PUT = "put"
    UPDATE_ACTION_DELETE = "delete"
    UPDATE_ACTION_ADD = "add"

    _allowed_actions = {UPDATE_ACTION_PUT, UPDATE_ACTION_DELETE,
                        UPDATE_ACTION_ADD}

    def __init__(self, action, value):
        """
        @param action: one of available action names
        @param value: AttributeValue instance, parameter for action
        """
        assert action in self._allowed_actions, (
            "Update action '%s' is't allowed" % action
        )

        self._action = action
        self._value = value

    @property
    def action(self):
        return self._action

    @property
    def value(self):
        return self._value


class IndexDefinition(ModelBase):
    _data_fields = ['index_name', 'attribute_to_index', 'projected_attributes']

    def __init__(self, index_name, attribute_to_index,
                 projected_attributes=None):
        """
        @param index_name: name of index
        @param attribute_to_index: attribute name to be indexed
        @param projected_attributes: set of non key attribute names to be
                    projected. If 'None' - all attributes will be projected
        """

        super(IndexDefinition, self).__init__()

        self._index_name = index_name
        self._attribute_to_index = attribute_to_index
        self._projected_attributes = (
            None if projected_attributes is None else
            frozenset(projected_attributes)
        )

    @property
    def index_name(self):
        return self._index_name

    @property
    def attribute_to_index(self):
        return self._attribute_to_index

    @property
    def projected_attributes(self):
        return self._projected_attributes


class SelectResult(object):

    def __init__(self, items=None, last_evaluated_key=None, count=None):
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

        self._items = items
        self._count = count
        self._last_evaluated_key = last_evaluated_key

    @property
    def items(self):
        return self._items

    @property
    def count(self):
        return self._count

    @property
    def last_evaluated_key(self):
        return self._last_evaluated_key


class ScanResult(SelectResult):

    def __init__(self, items=None, last_evaluated_key=None,
                 count=None, scanned_count=None):

        super(ScanResult, self).__init__(items, last_evaluated_key, count)

        self._scanned_count = scanned_count

    @property
    def scanned_count(self):
        return self._scanned_count


class TableSchema(ModelBase):
    _data_fields = ['table_name', 'attribute_defs', 'key_attributes',
                    'index_defs']

    def __init__(self, table_name, attribute_defs, key_attributes,
                 index_defs=None):
        """
        @param table_name: String, name of table to create
        @param attribute_defs: list of AttributeDefinition which define table
                    attribute names and types
        @param key_attrs: list of key attribute names, contains partition key
                    (the first in list, required) attribute name and extra key
                    attribute names (the second and other list items, not
                    required)

        @param index_defs: set of IndexDefinition which defines indexes on
                    table attributes
        """

        super(TableSchema, self).__init__()

        self._table_name = table_name
        self._attribute_defs = attribute_defs
        self._key_attributes = key_attributes
        self._index_defs = index_defs

    @property
    def table_name(self):
        return self._table_name

    @property
    def attribute_defs(self):
        return self._attribute_defs

    @property
    def key_attributes(self):
        return self._key_attributes

    @property
    def index_defs(self):
        return self._index_defs
