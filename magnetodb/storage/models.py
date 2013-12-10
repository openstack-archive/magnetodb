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


class ModelBase(object):

    _data_fields = []

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
        fields_as_list = []
        for field in self._data_fields:
            fields_as_list.append(self[field])
        return hash(tuple(fields_as_list))


class AttributeType(ModelBase):
    ELEMENT_TYPE_STRING = "string"
    ELEMENT_TYPE_NUMBER = "number"
    ELEMENT_TYPE_BLOB = "blob"

    COLLECTION_TYPE_SET = "set"

    _allowed_types = {ELEMENT_TYPE_STRING, ELEMENT_TYPE_NUMBER,
                      ELEMENT_TYPE_BLOB}

    _allowed_collection_types = {None, COLLECTION_TYPE_SET}

    _data_fields = ['element_type', '_collection_type']

    def __init__(self, element_type, collection_type=None):
        assert (element_type in self._allowed_types,
                "Attribute type '%s' is't allowed" % element_type)

        assert (collection_type in self._allowed_collection_types,
                "Attribute type collection '%s' is't allowed" %
                collection_type)

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


class AttributeDefinition(ModelBase):

    _data_fields = ['name']

    def __init__(self, attr_name, attr_type):
        self._name = attr_name
        self._type = attr_type

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type


class AttributeValue():

    def __init__(self, attr_type, attr_value):
        self._type = attr_type
        self._value = attr_value

    @property
    def value(self):
        return self._value

    @property
    def type(self):
        return self._type


class Condition():
    CONDITION_TYPE_EQUAL = "equal"

    _allowed_types = {CONDITION_TYPE_EQUAL}

    def __init__(self, condition_type, condition_arg):
        assert (condition_type in self._allowed_types,
                "Condition type '%s' is't allowed" % condition_type)

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

    _allowed_types = {Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_LESS,
                      CONDITION_TYPE_LESS_OR_EQUAL, CONDITION_TYPE_GREATER,
                      CONDITION_TYPE_GREATER_OR_EQUAL}

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


class ExpectedCondition(Condition):
    CONDITION_TYPE_EXISTS = "exists"

    _allowed_types = {Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_EXISTS}

    @classmethod
    def exists(cls):
        return cls(cls.CONDITION_TYPE_EXISTS, True)

    @classmethod
    def not_exists(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_EXISTS, False)


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


class UpdateItemAction():
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
        assert (action in self._allowed_actions,
                "Update action '%s' is't allowed" % action)

        self._action = action
        self._value = value

    @property
    def action(self):
        return self._action

    @property
    def value(self):
        return self._value


class TableSchema(ModelBase):
    _data_fields = ['table_name', 'attribute_defs', 'key_attributes',
                    'indexed_non_key_attributes']

    def __init__(self, table_name, attribute_defs, key_attributes,
                 indexed_non_key_attributes=None):
        """
        @param table_name: String, name of table to create
        @param attribute_defs: list of AttributeDefinition which define table
                    attribute names and types
        @param key_attrs: set of key attribute names, contains partitional_key
                    (the first in list, required) attribute name and extra key
                    attribute names (the second and other list items, not
                    required)

        @param indexed_non_key_attributes: set of non key attribute names to
                    be indexed
        """
        self._table_name = table_name
        self._attribute_defs = attribute_defs
        self._key_attributes = key_attributes
        self._indexed_non_key_attributes = indexed_non_key_attributes

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
    def indexed_non_key_attributes(self):
        return self._indexed_non_key_attributes
