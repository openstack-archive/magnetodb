

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
    CONDITION_TYPE_LESS = "less"
    CONDITION_TYPE_LESS_OR_EQUAL = "less_or_equal"
    CONDITION_TYPE_GREATER = "greater"
    CONDITION_TYPE_GREATER_OR_EQUAL = "greater_or_equal"

    _allowed_types = {CONDITION_TYPE_EQUAL, CONDITION_TYPE_LESS,
                      CONDITION_TYPE_LESS_OR_EQUAL, CONDITION_TYPE_GREATER,
                      CONDITION_TYPE_GREATER_OR_EQUAL}

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


class PreCondition(Condition):
    CONDITION_TYPE_EXISTS = "exists"

    _allowed_types = {Condition.CONDITION_TYPE_EQUAL, CONDITION_TYPE_EXISTS}

    @classmethod
    def exists(cls, attr_name):
        return cls(cls.CONDITION_TYPE_EXISTS, True)

    @classmethod
    def not_exists(cls, attr_name):
        return cls(cls.CONDITION_TYPE_EXISTS, False)
