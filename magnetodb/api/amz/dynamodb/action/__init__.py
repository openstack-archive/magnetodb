import jsonschema

from magnetodb.common import exception
from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)


class DynamoDBAction():
    schema = {}

    def __init__(self, context, action_params):
        self.validate_params(action_params)
        self.action_params = action_params
        self.context = context

    @classmethod
    def format_validation_msg(self, errors):
        # format path like object['field1'][i]['subfield2']
        messages = []
        for error in errors:
            path = list(error.path)
            f_path = "%s%s" % (path[0],
                               ''.join(['[%r]' % i for i in path[1:]]))
            messages.append("%s %s" % (f_path, error.message))
            for suberror in sorted(error.context, key=lambda e: e.schema_path):
                messages.append(suberror.message)
        error_msg = "; ".join(messages)
        return "Validation error: %s" % error_msg

    @classmethod
    def validate_params(cls, params):
            assert isinstance(params, dict)

            validator = jsonschema.Draft4Validator(cls.schema)
            if not validator.is_valid(params):
                errors = sorted(validator.iter_errors(params),
                                key=lambda e: e.path)
                error_msg = cls.format_validation_msg(errors)
                LOG.info(error_msg)
                raise exception.ValidationException(error_msg)

    @classmethod
    def perform(cls, context, action_params):
        cls.validate_params(action_params)

        return cls(context, action_params)()


class Props():
    TABLE_NAME = "TableName"
    ATTRIBUTE_DEFINITIONS = "AttributeDefinitions"
    ATTRIBUTE_NAME = "AttributeName"
    ATTRIBUTE_TYPE = "AttributeType"
    KEY_SCHEMA = "KeySchema"
    KEY_TYPE = "KeyType"
    LOCAL_SECONDARY_INDEXES = "LocalSecondaryIndexes"
    INDEX_NAME = "IndexName"
    PROJECTION = "Projection"
    NON_KEY_ATTRIBUTES = "NonKeyAttributes"
    PROJECTION_TYPE = "ProjectionType"
    PROVISIONED_THROUGHPUT = "ProvisionedThroughput"
    READ_CAPACITY_UNITS = "ReadCapacityUnits"
    WRITE_CAPACITY_UNITS = "WriteCapacityUnits"
    EXCLUSIVE_START_TABLE_NAME = "ExclusiveStartTableName"
    LIMIT = "Limit"


class Types():
    ATTRIBUTE_NAME = {
        "type": "string"
    }

    ATTRIBUTE_TYPE = {
        "type": "string"
    }

    KEY_TYPE = {
        "type": "string"
    }

    INDEX_NAME = {
        "type": "string"
    }

    TABLE_NAME = {
        "type": "string",
        "pattern": "'^\w+",
    }

    KEY_SCHEMA = {
        "type": "array",
        "items": {
            "type": "object",
            "required": [Props.ATTRIBUTE_NAME, Props.KEY_TYPE],
            "properties": {
                Props.ATTRIBUTE_NAME: ATTRIBUTE_NAME,
                Props.KEY_TYPE: KEY_TYPE
            }
        }
    }
