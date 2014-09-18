# Copyright 2014 Mirantis Inc.
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
from magnetodb.openstack.common.gettextutils import _
import re

from magnetodb.common.exception import ValidationError
from magnetodb.openstack.common.log import logging


LOG = logging.getLogger(__name__)

ATTRIBUTE_NAME_PATTERN = re.compile("^\w+")
TABLE_NAME_PATTERN = re.compile("^\w+")
INDEX_NAME_PATTERN = re.compile("^\w+")

WRONG_TYPE_MSG = _(
    "Wrong '%(property_name)s' type. %(json_type)s is expected, "
    "but %(prop_value)s found"
)


def __validate_type(value, property_name, py_type, json_type):
    if value is None:
        raise ValidationError(
            _("Required property '%(property_name)s' wasn't found "
              "or it's value is null"),
            property_name=property_name
        )

    if not isinstance(value, py_type):
        raise ValidationError(
            WRONG_TYPE_MSG,
            property_name=property_name,
            json_type=json_type,
            prop_value=json.dumps(value)
        )
    return value


def validate_string(value, property_name):
    return __validate_type(value, property_name, basestring, "String")


def validate_boolean(value, property_name):
    return __validate_type(value, property_name, bool, "Boolean")


def validate_integer(value, property_name, min_val=None, max_val=None):
    if isinstance(value, basestring):
        try:
            value = int(value)
        except ValueError:
            pass

    value = __validate_type(value, property_name, (int, long), "Integer")

    if min_val is not None and value < min_val:
        raise ValidationError(
            _("'%(property_name)s' property value[%(property_value)s] is less "
              "then min_value[%(min_value)s]."),
            property_name=property_name,
            property_value=value,
            min_value=min_val
        )
    if max_val is not None and value > max_val:
        raise ValidationError(
            _("'%(property_name)s' property value[%(property_value)s] is more "
              "then max_value[%(max_value)s]."),
            property_name=property_name,
            property_value=value,
            max_value=max_val
        )
    return value


def validate_object(value, property_name):
    return __validate_type(value, property_name, dict, "Object")


def validate_list(value, property_name):
    return __validate_type(value, property_name, list, "List")


def validate_set(value, property_name):
    validate_list(value, property_name)

    value_set = frozenset(value)
    if len(value_set) < len(value):
        raise ValidationError(
            WRONG_TYPE_MSG,
            property_name=property_name,
            json_type="List of unique values",
            prop_value=json.dumps(value)
        )
    return value_set


def validate_list_of_objects(value, property_name):
    validate_list(value, property_name)
    for item in value:
        if not isinstance(item, dict):
            raise ValidationError(
                WRONG_TYPE_MSG,
                property_name=property_name,
                json_type="List of Objects",
                prop_value=json.dumps(value)
            )
    return value


def validate_unexpected_props(value, property_name):
    if len(value) > 0:
        raise ValidationError(
            _("Unexpected properties were found for '%(property_name)s': "
              "%(unexpected_props)s"),
            property_name=property_name,
            unexpected_props=json.dumps(value)
        )
    return value


def validate_attr_name(value):
    validate_string(value, "attribute name")

    if not ATTRIBUTE_NAME_PATTERN.match(value):
        raise ValidationError(
            _("Wrong attribute name '%(prop_value)s' found"),
            prop_value=value
        )


def validate_table_name(value):
    validate_string(value, "table name")

    if not TABLE_NAME_PATTERN.match(value):
        raise ValidationError(
            _("Wrong table name '%(prop_value)s' found"),
            prop_value=value
        )
    return value


def validate_index_name(value):
    validate_string(value, "index name")

    if not INDEX_NAME_PATTERN.match(value):
        raise ValidationError(
            _("Wrong index name '%(prop_value)s' found"),
            prop_value=value
        )
    return value
