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
import re

from magnetodb.common.exception import ValidationError
from magnetodb.openstack.common.log import logging


LOG = logging.getLogger(__name__)

ATTRIBUTE_NAME_PATTERN = re.compile("^\w+")
TABLE_NAME_PATTERN = re.compile("^\w+")
INDEX_NAME_PATTERN = re.compile("^\w+")


def validate_string(value, property_name):
    if value is None:
        raise ValidationError(
            "Required property '%(property_name)s' wasn't found "
            "or it's value is null",
            property_name=property_name
        )

    if not isinstance(value, basestring):
        raise ValidationError(
            "Wrong '%(property_name)s' type. "
            "String is expected, but %(prop_value)s found",
            property_name=property_name,
            prop_value=json.dumps(value)
        )


def validate_boolean(value, property_name):
    if value is None:
        raise ValidationError(
            "Required property '%(property_name)s' wasn't found "
            "or it's value is null",
            property_name=property_name
        )

    if not isinstance(value, bool):
        raise ValidationError(
            "Wrong '%(property_name)s' type. "
            "Boolean is expected, but %(prop_value)s found",
            property_name=property_name,
            prop_value=json.dumps(value)
        )


def validate_integer(value, property_name):
    if value is None:
        raise ValidationError(
            "Required property '%(property_name)s' wasn't found "
            "or it's value is null",
            property_name=property_name
        )

    if not isinstance(value, (int, long)):
        raise ValidationError(
            "Wrong '%(property_name)s' type. "
            "Integer is expected, but %(prop_value)s found",
            property_name=property_name,
            prop_value=json.dumps(value)
        )


def validate_list(value, property_name):
    if value is None:
        raise ValidationError(
            "Required property '%(property_name)s' wasn't found "
            "or it's value is null",
            property_name=property_name
        )

    if not hasattr(value, "__iter__"):
        raise ValidationError(
            "Wrong '%(property_name)s' value type. "
            "List is expected, but %(prop_value)s found",
            property_name=property_name,
            prop_value=json.dumps(value)
        )


def validate_object(value, property_name):
    if value is None:
        raise ValidationError(
            "Required property '%(property_name)s' wasn't found "
            "or it's value is null",
            property_name=property_name
        )
    if not isinstance(value, dict):
        raise ValidationError(
            "Wrong '%(property_name)s' property value type. "
            "Object is expected, but %(prop_value)s found",
            property_name=property_name,
            prop_value=json.dumps(value)
        )


def validate_list_of_objects(value, property_name):
    validate_list(value, property_name)
    for item in value:
        if not isinstance(item, dict):
            raise ValidationError(
                "Wrong '%(property_name)s' value type. "
                "List of Objects is expected, but %(prop_value)s found",
                property_name=property_name,
                prop_value=json.dumps(value)
            )


def validate_unexpected_props(value, property_name):
    if len(value) > 0:
        raise ValidationError(
            "Unexpected properties were found for '%(property_name)s': "
            "%(unexpected_props)s",
            property_name=property_name,
            unexpected_props=json.dumps(value)
        )


def validate_attr_name(value):
    validate_string(value, "attribute name")

    if not ATTRIBUTE_NAME_PATTERN.match(value):
        raise ValidationError(
            "Wrong attribute name '%(prop_value)s' found",
            prop_value=value
        )


def validate_table_name(value):
    validate_string(value, "table name")

    if not TABLE_NAME_PATTERN.match(value):
        raise ValidationError(
            "Wrong table name '%(prop_value)s' found",
            prop_value=value
        )


def validate_index_name(value):
    validate_string(value, "index name")

    if not INDEX_NAME_PATTERN.match(value):
        raise ValidationError(
            "Wrong index name '%(prop_value)s' found",
            prop_value=value
        )

