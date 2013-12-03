

class AttributeType():
    ATTRIBUTE_TYPE_STRING = "string"
    ATTRIBUTE_TYPE_NUMBER = "number"
    ATTRIBUTE_TYPE_BLOB = "blob"

    ATTRIBUTE_COLLECTION_TYPE_SET = "set"

    _allowed_types = {ATTRIBUTE_TYPE_STRING, ATTRIBUTE_TYPE_NUMBER,
                      ATTRIBUTE_TYPE_BLOB}

    _allowed_collection_types = {None, ATTRIBUTE_COLLECTION_TYPE_SET}

    def __init__(self, element_type, collection_type=None):
        assert (element_type in self._allowed_types,
                "Attribute type '%s' is't allowed" % element_type)

        assert (collection_type in self._allowed_collection_types,
                "Attribute type collection '%s' is't allowed" %
                collection_type)

        self.element_type = element_type
        self._collection_type = collection_type

    @property
    def element_type(self):
        return self._type

    @property
    def collection_type(self):
        return self._collection_type


class AttributeDefinition():
    def __init__(self, attr_name, attr_type):
        assert (attr_type.collection_type is None,
                "Can't use collection in schema attribute definition")

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

    def __init__(self, condition_type, attr_name, condition_arg):
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
        return cls(cls.CONDITION_TYPE_EQUALITY, condition_arg)


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


class NotIndexedCondition(Condition):
    pass


class WriteItemRequest():
    def __init__(self, table_name, timestamp):
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


class DeleteItemRequest(WriteItemRequest):
    def __init__(self, table_name, indexed_condition_map,
                 not_indexed_condition_map=None):
        """
        @param table_name: String, name of table to delete item from
        @param indexed_condition_map: indexed attribute name to
                    IndexedCondition instance mapping. It defines rows
                    set to be removed
        """
        super(DeleteItemRequest, self).__init__(table_name)

        self._indexed_condition_map = indexed_condition_map

    @property
    def indexed_condition_map(self):
        return self._indexed_condition_map


class PutItemRequest(WriteItemRequest):
    def __init__(self, table_name, key_map, if_not_exist=False):
        """
        @param table_name: String, name of table to delete item from
        @param key_map: key attribute name to
                    key attribute value mapping. It defines row's it to put
                    item
        """
        super(PutItemRequest, self).__init__(table_name)

        self._key_map = key_map

    @property
    def key_map(self):
        return self._key_map


class UpdateItemRequest(WriteItemRequest):
    def __init__(self, table_name, key_map, not_indexed_condition_map=None):
        """
        @param table_name: String, name of table to delete item from
        @param key_map: key attribute name to
                    key attribute value mapping. It defines row's it to put
                    item
        """
        super(PutItemRequest, self).__init__(table_name)

        self._key_map = key_map

    @property
    def key_map(self):
        return self._key_map


class TableSchema():
    def __init__(self, table_name, attribute_defs, key_attributes,
                 indexed_non_key_attributes=None):
        """
        @param table_name: String, name of table to create
        @param attribute_defs: list of AttributeDefinition which define table
                    attribute names and types
        @param key_attrs: list of key attribute names, contains partitional_key
                    (the first in list, required) attribute name and extra key
                    attribute names (the second and other list items, not
                    required)

        @param indexed_non_key_attributes: list of non key attribute names to
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
