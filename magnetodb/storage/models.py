# Copyright 2013 Mirantis Inc.
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

import base64
import blist
import datetime
import decimal
import sys
import uuid

from oslo_serialization import jsonutils as json
from oslo_utils import timeutils

from magnetodb.common import exception
from magnetodb.i18n import _

DECIMAL_CONTEXT = decimal.Context(
    prec=38, rounding=None,
    traps=[],
    flags=[],
    Emax=126,
    Emin=-128
)


class ModelBase(object):

    def __repr__(self):
        return self.to_json(add_model_meta_info=False)

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
            cmp(self._get_model_data(), other._get_model_data()) == 0
        )

    def __ne__(self, other):
        return not self == other

    def __hash__(self):
        if not self._hash:
            self._hash = hash(tuple(sorted(self._get_model_data().items())))

        return self._hash

    def _get_model_data(self):
        return self._data

    def to_json(self, add_model_meta_info=True):
        def encode_model(obj):
            if isinstance(obj, ModelBase):
                data = obj._get_model_data().copy()
                if add_model_meta_info:
                    data["__model__"] = obj.__class__.__name__
                return data
            if hasattr(obj, "__iter__"):
                return list(obj)
            if isinstance(obj, (decimal.Decimal, uuid.UUID)):
                return str(obj)
            if isinstance(obj, datetime.datetime):
                return timeutils.isotime(obj, subsecond=True)
            raise TypeError(repr(obj) + " is not JSON serializable")

        return json.dumps(self, default=encode_model, sort_keys=True)

    @classmethod
    def from_json(cls, jsn):
        def as_model(dct):
            model_cls = dct.pop('__model__', None)
            if model_cls:
                return eval(model_cls)(**dct)
            return dct

        res = json.loads(jsn, object_hook=as_model)
        assert isinstance(res, cls)
        return res


class AttributeType(ModelBase):
    PRIMITIVE_TYPE_STRING = "S"
    PRIMITIVE_TYPE_NUMBER = "N"
    PRIMITIVE_TYPE_BLOB = "B"

    COLLECTION_TYPE_SET = "S"
    COLLECTION_TYPE_MAP = "M"

    __cache = dict()

    VALIDATION_ERROR_PATTERN = _("Attribute type '%(type)s' is not recognized")

    _allowed_primitive_types = set(
        [PRIMITIVE_TYPE_STRING, PRIMITIVE_TYPE_NUMBER, PRIMITIVE_TYPE_BLOB]
    )

    _allowed_collection_types = set(
        [None, COLLECTION_TYPE_SET, COLLECTION_TYPE_MAP]
    )

    def validate(self, attr_type):
        if not isinstance(attr_type, basestring):
            raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                            type=attr_type)

        if len(attr_type) == 1:
            if attr_type not in self._allowed_primitive_types:
                raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                                type=attr_type)
            return

        collection_type = attr_type[-1]
        if collection_type not in self._allowed_collection_types:
            raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                            type=attr_type)

        if collection_type == self.COLLECTION_TYPE_MAP:
            if len(attr_type) != 3:
                raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                                type=attr_type)
            key_type = attr_type[0]
            value_type = attr_type[1]
            if (key_type not in self._allowed_primitive_types or
                    value_type not in self._allowed_primitive_types):
                raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                                type=attr_type)
            return
        if len(attr_type) != 2:
            raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                            type=attr_type)
        element_type = attr_type[0]
        if element_type not in self._allowed_primitive_types:
            raise exception.ValidationError(self.VALIDATION_ERROR_PATTERN,
                                            type=attr_type)

    def __new__(cls, type):
        attr_type = cls.__cache.get(type, None)

        if attr_type:
            return attr_type
        attr_type = object.__new__(cls, type)
        cls.__cache[type] = attr_type

        return attr_type

    def __init__(self, type):
        self.validate(type)
        super(AttributeType, self).__init__(type=type)

    @property
    def collection_type(self):
        return self.type[-1] if len(self.type) > 1 else None

    @property
    def element_type(self):
        return self.type[0] if len(self.type) == 2 else None

    @property
    def key_type(self):
        return self.type[0] if len(self.type) == 3 else None

    @property
    def value_type(self):
        return self.type[1] if len(self.type) == 3 else None


ORDER_TYPE_ASC = "ASC"
ORDER_TYPE_DESC = "DESC"


class AttributeValue(ModelBase):
    def __init__(self, attr_type, value=None, encoded_value=None,
                 decoded_value=None):
        if not isinstance(attr_type, AttributeType):
            attr_type = AttributeType(attr_type)

        if value is not None:
            decoded_value = self.__decode_value(attr_type, value)
            encoded_value = None

        super(AttributeValue, self).__init__(
            attr_type=attr_type, encoded_value=encoded_value,
            decoded_value=decoded_value
        )

    def __eq__(self, other):
        return (
            type(other) == type(self) and
            self.attr_type == other.attr_type and
            self.decoded_value == other.decoded_value
        )

    def __hash__(self):
        if not self._hash:
            self._hash = hash((self.attr_type, self.decoded_value))

        return self._hash

    def _get_model_data(self):
        return {
            "attr_type": self.attr_type,
            "value": self.encoded_value
        }

    @staticmethod
    def __decode_single_value(value_type, value):
        if value_type == AttributeType.PRIMITIVE_TYPE_STRING:
            if isinstance(value, basestring):
                return value
        elif value_type == AttributeType.PRIMITIVE_TYPE_NUMBER:
            if isinstance(value, basestring):
                try:
                    return int(value)
                except ValueError:
                    try:
                        return DECIMAL_CONTEXT.create_decimal(value)
                    except decimal.ConversionSyntax:
                        pass
            elif isinstance(value, (int, long, decimal.Decimal)):
                return value
        elif value_type == AttributeType.PRIMITIVE_TYPE_BLOB:
            if isinstance(value, basestring):
                return base64.b64decode(value)

        return None

    @classmethod
    def __decode_value(cls, attr_type, encoded_value):
        decoded_value = None
        if decoded_value is not None:
            return decoded_value

        collection_type = attr_type.collection_type
        if collection_type is None:
            decoded_value = cls.__decode_single_value(attr_type.type,
                                                      encoded_value)
        elif collection_type == AttributeType.COLLECTION_TYPE_MAP:
            if isinstance(encoded_value, dict):
                res_dict = dict()
                key_type = attr_type.key_type
                value_type = attr_type.value_type
                for key, value in encoded_value.iteritems():
                    res_dict[cls.__decode_single_value(key_type, key)] = (
                        cls.__decode_single_value(value_type, value)
                    )
                decoded_value = res_dict
        elif collection_type == AttributeType.COLLECTION_TYPE_SET:
            element_type = attr_type.element_type
            res = blist.sortedset()
            for val in encoded_value:
                res.add(cls.__decode_single_value(element_type, val))
            decoded_value = res

        if decoded_value is None:
            raise exception.ValidationError(
                _("Can't recognize attribute value '%(value)s'"
                  "of type %(type)s"),
                type=attr_type, value=json.dumps(encoded_value)
            )

        return decoded_value

    @property
    def decoded_value(self):
        decoded_value = self._data["decoded_value"]
        if decoded_value is not None:
            return decoded_value
        encoded_value = self._data["encoded_value"]
        attr_type = self._data["attr_type"]

        decoded_value = self.__decode_value(attr_type, encoded_value)

        self._data["decoded_value"] = decoded_value
        return decoded_value

    @staticmethod
    def __encode_single_value(value_type, value):
        if value_type == AttributeType.PRIMITIVE_TYPE_STRING:
            return value
        elif value_type == AttributeType.PRIMITIVE_TYPE_NUMBER:
            return str(value)
        elif value_type == AttributeType.PRIMITIVE_TYPE_BLOB:
            return base64.b64encode(value)

        return None

    @classmethod
    def __encode_value(cls, attr_type, decoded_value):
        if attr_type.collection_type is None:
            return cls.__encode_single_value(attr_type.type, decoded_value)
        elif attr_type.collection_type == AttributeType.COLLECTION_TYPE_MAP:
            key_type = attr_type.key_type
            value_type = attr_type.value_type
            res = dict()
            for key, value in decoded_value.iteritems():
                res[cls.__encode_single_value(key_type, key)] = (
                    cls.__encode_single_value(value_type, value)
                )
            return res
        else:
            element_type = attr_type.element_type
            return [
                cls.__encode_single_value(element_type, val)
                for val in decoded_value
            ]

    @property
    def encoded_value(self):
        encoded_value = self._data["encoded_value"]
        if encoded_value is not None:
            return encoded_value
        decoded_value = self._data["decoded_value"]
        attr_type = self._data["attr_type"]

        encoded_value = self.__encode_value(attr_type, decoded_value)

        self._data["encoded_value"] = encoded_value
        return encoded_value

    @property
    def is_str(self):
        return self.attr_type.type == AttributeType.PRIMITIVE_TYPE_STRING

    @property
    def is_number(self):
        return self.attr_type.type == AttributeType.PRIMITIVE_TYPE_NUMBER

    @property
    def is_set(self):
        return (self.attr_type.collection_type ==
                AttributeType.COLLECTION_TYPE_SET)

    @property
    def is_map(self):
        return (self.attr_type.collection_type ==
                AttributeType.COLLECTION_TYPE_MAP)


class Condition(ModelBase):
    CONDITION_TYPE_EQUAL = "EQ"

    _allowed_types_to_arg_count_map = {
        CONDITION_TYPE_EQUAL: (1, 1)
    }

    _types_with_only_primitive_arg = set()

    def __init__(self, type, args):
        allowed_arg_count = self._allowed_types_to_arg_count_map.get(type,
                                                                     None)
        if allowed_arg_count is None:
            raise exception.ValidationError(
                _("%(condition_class)s of type['%(type)s'] is not allowed"),
                condition_class=self.__class__.__name__, type=type)

        actual_arg_count = len(args) if args is not None else 0

        if (actual_arg_count < allowed_arg_count[0] or
                actual_arg_count > allowed_arg_count[1]):
            if allowed_arg_count[0] == allowed_arg_count[1]:
                raise exception.ValidationError(
                    _("%(condition_class)s of type['%(type)s'] requires "
                      "exactly %(allowed_arg_count)s arguments, "
                      "but %(actual_arg_count)s found"),
                    condition_class=self.__class__.__name__, type=type,
                    allowed_arg_count=allowed_arg_count[0],
                    actual_arg_count=actual_arg_count
                )
            else:
                raise exception.ValidationError(
                    _("%(condition_class)s of type['%(type)s'] requires from "
                      "%(min_args_allowed)s to %(max_args_allowed)s arguments "
                      "provided, but %(actual_arg_count)s found"),
                    condition_class=self.__class__.__name__, type=type,
                    min_args_allowed=allowed_arg_count[0],
                    max_args_allowed=allowed_arg_count[1],
                    actual_arg_count=actual_arg_count
                )

        if args is not None and type in self._types_with_only_primitive_arg:
            for arg in args:
                if arg.attr_type.collection_type is not None:
                    raise exception.ValidationError(
                        _("%(condition_class)s of type['%(type)s'] allows "
                          "only primitive arguments"),
                        condition_class=self.__class__.__name__, type=type
                    )

        super(Condition, self).__init__(type=type, args=args)

    @classmethod
    def eq(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_EQUAL, (condition_arg,))

    @property
    def arg(self):
        if self.args is None:
            return None
        assert len(self.args) == 1

        return self.args[0]


class IndexedCondition(Condition):
    CONDITION_TYPE_LESS = "LT"
    CONDITION_TYPE_LESS_OR_EQUAL = "LE"
    CONDITION_TYPE_GREATER = "GT"
    CONDITION_TYPE_GREATER_OR_EQUAL = "GE"

    _allowed_types_to_arg_count_map = {
        Condition.CONDITION_TYPE_EQUAL: (1, 1),
        CONDITION_TYPE_LESS: (1, 1),
        CONDITION_TYPE_LESS_OR_EQUAL: (1, 1),
        CONDITION_TYPE_GREATER: (1, 1),
        CONDITION_TYPE_GREATER_OR_EQUAL: (1, 1)
    }

    _types_with_only_primitive_arg = _allowed_types_to_arg_count_map.keys()

    @classmethod
    def lt(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_LESS, (condition_arg,))

    @classmethod
    def le(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_LESS_OR_EQUAL, (condition_arg,))

    @classmethod
    def gt(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_GREATER, (condition_arg,))

    @classmethod
    def ge(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_GREATER_OR_EQUAL, (condition_arg,))

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

    CONDITION_TYPE_IN = "IN"
    CONDITION_TYPE_CONTAINS = "CONTAINS"
    CONDITION_TYPE_NOT_CONTAINS = "NOT_CONTAINS"
    CONDITION_TYPE_NOT_EQUAL = "NE"
    CONDITION_TYPE_NOT_NULL = "NOT_NULL"
    CONDITION_TYPE_NULL = "NULL"

    _allowed_types_to_arg_count_map = {
        Condition.CONDITION_TYPE_EQUAL: (1, 1),
        IndexedCondition.CONDITION_TYPE_LESS: (1, 1),
        IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL: (1, 1),
        IndexedCondition.CONDITION_TYPE_GREATER: (1, 1),
        IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL: (1, 1),
        CONDITION_TYPE_IN: (1, sys.maxint),
        CONDITION_TYPE_CONTAINS: (1, 1),
        CONDITION_TYPE_NOT_CONTAINS: (1, 1),
        CONDITION_TYPE_NOT_EQUAL: (1, 1),
        CONDITION_TYPE_NOT_NULL: (0, 0),
        CONDITION_TYPE_NULL: (0, 0)
    }

    _types_with_only_primitive_arg = {
        IndexedCondition.CONDITION_TYPE_LESS,
        IndexedCondition.CONDITION_TYPE_LESS_OR_EQUAL,
        IndexedCondition.CONDITION_TYPE_GREATER,
        IndexedCondition.CONDITION_TYPE_GREATER_OR_EQUAL,
        CONDITION_TYPE_IN,
        CONDITION_TYPE_CONTAINS,
        CONDITION_TYPE_NOT_CONTAINS
    }

    @classmethod
    def neq(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_NOT_EQUAL, (condition_arg,))

    @classmethod
    def in_set(cls, condition_args):
        return cls(cls.CONDITION_TYPE_IN, condition_args)

    @classmethod
    def contains(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_CONTAINS, (condition_arg,))

    @classmethod
    def not_contains(cls, condition_arg):
        return cls(cls.CONDITION_TYPE_NOT_CONTAINS, (condition_arg,))

    @classmethod
    def null(cls):
        return cls(cls.CONDITION_TYPE_NULL, None)

    @classmethod
    def not_null(cls):
        return cls(cls.CONDITION_TYPE_NOT_NULL, None)


class ExpectedCondition(ScanCondition):
    _allowed_types_to_arg_count_map = {
        Condition.CONDITION_TYPE_EQUAL: (1, 1),
        ScanCondition.CONDITION_TYPE_NOT_NULL: (0, 0),
        ScanCondition.CONDITION_TYPE_NULL: (0, 0)
    }


class SelectType(ModelBase):
    SELECT_TYPE_ALL = "ALL_ATTRIBUTES"
    SELECT_TYPE_ALL_PROJECTED = "ALL_PROJECTED_ATTRIBUTES"
    SELECT_TYPE_SPECIFIC = "SPECIFIC_ATTRIBUTES"
    SELECT_TYPE_COUNT = "COUNT"

    _allowed_types = set([SELECT_TYPE_ALL, SELECT_TYPE_ALL_PROJECTED,
                          SELECT_TYPE_SPECIFIC, SELECT_TYPE_COUNT])

    def __init__(self, select_type, attributes=None):
        if select_type not in self._allowed_types:
            raise exception.ValidationError(
                _("Select type '%(select_type)s' isn't allowed"),
                select_type=select_type
            )

        if attributes is not None:
            if select_type != self.SELECT_TYPE_SPECIFIC:
                raise exception.ValidationError(
                    _("Attribute list is only expected with select_type "
                      "'%(select_type)s'"),
                    select_type=self.SELECT_TYPE_SPECIFIC
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
    def specific_attributes(cls, attributes):
        return cls(cls.SELECT_TYPE_SPECIFIC, attributes)

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


class WriteItemRequest(ModelBase):
    WRITE_REQUEST_TYPE_PUT = "PUT"
    WRITE_REQUEST_TYPE_DELETE = "DELETE"

    _allowed_types = set([WRITE_REQUEST_TYPE_PUT, WRITE_REQUEST_TYPE_DELETE])

    def __init__(self, type, attribute_map):
        """
        :param type: one of available type names
        :param attribute_map: map of attribute name to AttributeValue instance,
                represents item to put or key to delete
        """

        if type not in self._allowed_types:
            raise exception.ValidationError(
                _("Write request_type '%(type)s' isn't allowed"),
                type=type
            )

        super(WriteItemRequest, self).__init__(
            type=type, attribute_map=attribute_map
        )

    @ classmethod
    def put(cls, attribute_map):
        return cls(cls.WRITE_REQUEST_TYPE_PUT, attribute_map)

    @ classmethod
    def delete(cls, attribute_map):
        return cls(cls.WRITE_REQUEST_TYPE_DELETE, attribute_map)

    @property
    def is_put(self):
        return self.type == self.WRITE_REQUEST_TYPE_PUT

    @property
    def is_delete(self):
        return self.type == self.WRITE_REQUEST_TYPE_DELETE


class GetItemRequest(ModelBase):
    def __init__(self, table_name, key_attribute_map, attributes_to_get,
                 consistent, encoded_json=None):
        """
        :param table_name: String, name of table to get item from
        :param attribute_map: attribute name to AttributeValue mapping.
        """
        super(GetItemRequest, self).__init__(
            table_name=table_name,
            key_attribute_map=key_attribute_map,
            attributes_to_get=attributes_to_get,
            consistent=consistent
        )


class UpdateItemAction(ModelBase):
    UPDATE_ACTION_PUT = "PUT"
    UPDATE_ACTION_DELETE = "DELETE"
    UPDATE_ACTION_ADD = "ADD"

    _allowed_actions = set([UPDATE_ACTION_PUT, UPDATE_ACTION_DELETE,
                            UPDATE_ACTION_ADD])

    def __init__(self, action, value):
        """
        :param action: one of available action names
        :param value: AttributeValue instance, parameter for action
        """

        if action not in self._allowed_actions:
            raise exception.ValidationError(
                _("Update action '%(action)s' isn't allowed"),
                action=action
            )

        super(UpdateItemAction, self).__init__(action=action, value=value)


class DeleteReturnValuesType(ModelBase):
    RETURN_VALUES_TYPE_NONE = "NONE"
    RETURN_VALUES_TYPE_ALL_OLD = "ALL_OLD"
    _allowed_types = set(
        [RETURN_VALUES_TYPE_NONE, RETURN_VALUES_TYPE_ALL_OLD]
    )

    def __init__(self, type):
        """
        :param type: one of available return values type
        """
        if type not in self._allowed_types:
            raise exception.ValidationError(
                _("Return values type '%(type)s' isn't allowed"),
                type=type
            )

        super(DeleteReturnValuesType, self).__init__(type=type)


class InsertReturnValuesType(DeleteReturnValuesType):
    RETURN_VALUES_TYPE_ALL_NEW = "ALL_NEW"

    _allowed_types = set(
        [DeleteReturnValuesType.RETURN_VALUES_TYPE_NONE,
         DeleteReturnValuesType.RETURN_VALUES_TYPE_ALL_OLD,
         RETURN_VALUES_TYPE_ALL_NEW]
    )


class UpdateReturnValuesType(InsertReturnValuesType):
    RETURN_VALUES_TYPE_UPDATED_OLD = "UPDATED_OLD"
    RETURN_VALUES_TYPE_UPDATED_NEW = "UPDATED_NEW"

    _allowed_types = set(
        [DeleteReturnValuesType.RETURN_VALUES_TYPE_NONE,
         DeleteReturnValuesType.RETURN_VALUES_TYPE_ALL_OLD,
         InsertReturnValuesType.RETURN_VALUES_TYPE_ALL_NEW,
         RETURN_VALUES_TYPE_UPDATED_OLD,
         RETURN_VALUES_TYPE_UPDATED_NEW]
    )


class IndexDefinition(ModelBase):
    def __init__(self, alt_hash_key_attr, alt_range_key_attr,
                 projected_attributes=None):
        """
        :param alt_hash_key_attr: name of index hash key
        :param alt_range_key_attr: name of index range key
        :param projected_attributes: set of non key attribute names to be
                    projected. If 'None' - all attributes will be projected
        """
        projected_attributes = (
            None if projected_attributes is None else
            frozenset(projected_attributes)
        )

        super(IndexDefinition, self).__init__(
            alt_hash_key_attr=alt_hash_key_attr,
            alt_range_key_attr=alt_range_key_attr,
            projected_attributes=projected_attributes
        )


class SelectResult(ModelBase):

    def __init__(self, items=None, last_evaluated_key=None, count=None,
                 **kwargs):
        """
        :param items: list of attribute name to AttributeValue mappings
        :param last_evaluated_key: attribute name to AttributeValue mapping,
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
        :param attribute_type_map: attribute name to AttributeType mapping
        :param key_attrs: list of key attribute names, contains partition key
                    (the first in list, required) attribute name and extra key
                    attribute names (the second and other list items, not
                    required)
        :param index_def_map: index name to IndexDefinition mapping
        """

        if index_def_map is None:
            index_def_map = {}

        for key_attr in key_attributes:
            if key_attr not in attribute_type_map:
                raise exception.ValidationError(
                    "Definition for attribute['%(attr_name)s'] wasn't found",
                    attr_name=key_attr
                )
            # Validate the primary hash and range key types, make sure them are
            # scalar types
            attr_type = attribute_type_map.get(key_attr, None)
            if attr_type is not None and attr_type.collection_type is not None:
                raise exception.ValidationError(
                    _("Type '%(prop_value)s' is not a scalar type"),
                    prop_value=attr_type['type'])

        if len(key_attr) < 2 and index_def_map:
            raise exception.ValidationError("Local secondary indexes are not "
                                            "allowed for tables with hash "
                                            "key only")

        for index_name, index_def in index_def_map.iteritems():
            if index_def.alt_hash_key_attr != key_attributes[0]:
                msg = _("Hash key of index '%(index_name)s' must "
                        "be the same as primary key's hash key.")
                raise exception.ValidationError(msg, index_name=index_name)

            if index_def.alt_range_key_attr not in attribute_type_map:
                raise exception.ValidationError(
                    "Definition for attribute['%(attr_name)s'] wasn't found",
                    attr_name=index_def.alt_range_key_attr
                )

        super(TableSchema, self).__init__(
            attribute_type_map=attribute_type_map,
            key_attributes=key_attributes,
            index_def_map=index_def_map)

    @property
    def hash_key_name(self):
        return self.key_attributes[0]

    @property
    def range_key_name(self):
        try:
            return self.key_attributes[1]
        except IndexError:
            return None


class TableMeta(ModelBase):
    TABLE_STATUS_CREATING = "CREATING"
    TABLE_STATUS_DELETING = "DELETING"
    TABLE_STATUS_ACTIVE = "ACTIVE"
    TABLE_STATUS_CREATE_FAILED = "CREATE_FAILED"
    TABLE_STATUS_DELETE_FAILED = "DELETE_FAILED"

    _allowed_statuses = set([TABLE_STATUS_CREATING, TABLE_STATUS_DELETING,
                             TABLE_STATUS_ACTIVE, TABLE_STATUS_CREATE_FAILED,
                             TABLE_STATUS_DELETE_FAILED])

    def __init__(self, id, schema, status, creation_date_time):
        """
        :param id: table id
        :param schema: TableSchema instance
        :param status: table status
        :param creation_date_time: table creation timestamp
        """

        assert status in self._allowed_statuses, (
            "Table status '%s' isn't allowed" % status
        )
        if isinstance(id, basestring):
            id = uuid.UUID(id)
        if isinstance(creation_date_time, basestring):
            creation_date_time = timeutils.parse_isotime(creation_date_time)
        super(TableMeta, self).__init__(id=id, schema=schema,
                                        status=status,
                                        creation_date_time=creation_date_time)


class BackupMeta(ModelBase):
    BACKUP_STATUS_CREATING = "CREATING"
    BACKUP_STATUS_DELETING = "DELETING"
    BACKUP_STATUS_CREATED = "CREATED"
    BACKUP_STATUS_CREATE_FAILED = "CREATE_FAILED"
    BACKUP_STATUS_DELETE_FAILED = "DELETE_FAILED"

    _allowed_statuses = set([BACKUP_STATUS_CREATING,
                             BACKUP_STATUS_DELETING,
                             BACKUP_STATUS_CREATED,
                             BACKUP_STATUS_CREATE_FAILED,
                             BACKUP_STATUS_DELETE_FAILED])

    def __init__(self, id, name, table_name, status, location,
                 start_date_time=None, finish_date_time=None, strategy={}):

        assert status in self._allowed_statuses, (
            "Backup status '%s' is not allowed" % status
        )
        if isinstance(id, basestring):
            id = uuid.UUID(id)
        if isinstance(start_date_time, basestring):
            start_date_time = timeutils.parse_isotime(start_date_time)
        if isinstance(finish_date_time, basestring):
            finish_date_time = timeutils.parse_isotime(finish_date_time)

        super(BackupMeta, self).__init__(
            id=id,
            name=name,
            table_name=table_name,
            status=status,
            start_date_time=start_date_time,
            finish_date_time=finish_date_time,
            location=location,
            strategy=strategy
        )


class RestoreJobMeta(ModelBase):
    RESTORE_STATUS_RESTORING = "RESTORING"
    RESTORE_STATUS_RESTORED = "RESTORED"
    RESTORE_STATUS_RESTORE_FAILED = "RESTORE_FAILED"

    _allowed_statuses = set([RESTORE_STATUS_RESTORING,
                             RESTORE_STATUS_RESTORED,
                             RESTORE_STATUS_RESTORE_FAILED])

    def __init__(self, id, table_name, status, backup_id=None, source=None,
                 start_date_time=None, finish_date_time=None):

        assert status in self._allowed_statuses, (
            "Restore job  status '%s' is not allowed" % status
        )
        if isinstance(id, basestring):
            id = uuid.UUID(id)
        if isinstance(backup_id, basestring):
            backup_id = uuid.UUID(backup_id)
        if isinstance(start_date_time, basestring):
            start_date_time = timeutils.parse_isotime(start_date_time)
        if isinstance(finish_date_time, basestring):
            finish_date_time = timeutils.parse_isotime(finish_date_time)

        super(RestoreJobMeta, self).__init__(
            id=id,
            table_name=table_name,
            status=status,
            backup_id=backup_id,
            source=source,
            start_date_time=start_date_time,
            finish_date_time=finish_date_time
        )
