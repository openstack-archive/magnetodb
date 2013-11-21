import jsonschema

from magnetodb.common import exception
from magnetodb.openstack.common.log import logging
from magnetodb import storage

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


class ListTablesDynamoDBAction(DynamoDBAction):
    EXCLUSIVE_START_TABLE_NAME_PARAM = "ExclusiveStartTableName"
    LIMIT_PARAM = "Limit"

    schema = {
        EXCLUSIVE_START_TABLE_NAME_PARAM: {
            "type": "string",
            "pattern": "'^\w+",
            "required": False,
        },
        LIMIT_PARAM: {
            "type": "integer",
            "minimum": 0,
        }
    }

    def __call__(self):
        exclusive_start_table_name = (
            self.action_params.get(self.EXCLUSIVE_START_TABLE_NAME_PARAM, None)
        )

        limit = self.action_params.get(self.LIMIT_PARAM, None)

        table_names = (
            storage.list_tables(self.context,
                exclusive_start_table_name=exclusive_start_table_name,
                limit=limit)
        )

        if table_names:
            return {"LastEvaluatedTableName": table_names[-1],
                    "TableNames": table_names}
        else:
            return {}
