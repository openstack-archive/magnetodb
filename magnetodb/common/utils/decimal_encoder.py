# Copyright 2014 Symantec Corporation.
#  All Rights Reserved.
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
#
#

import decimal

from oslo_serialization import jsonutils as json


class DecimalEncoder(json.JSONEncoder):
    """
    DecimalEncoder extends the original Extensible JSONEncoder
    to handle Decimal data type, which is used for holding MagnetoDB
    DateTime data type.

    Note that we can not directly use the default method as passed in
    from openstack.common.jsonutils. Decimal is not handled by jsonutils'
    to_primitive method. Calling jsonutils.to_primitive(payload,
    convert_instances=True) would fail if the payload contains Decimal
    data type.

    We need to enhance the default function instead of overwriting it.

    The enhancement is to handle Decimal data type, while delegate the
    other data types to jsonutils's to_primitive function.

    """

    def __init__(self, skipkeys=False,
                 ensure_ascii=True,
                 check_circular=True,
                 allow_nan=True,
                 sort_keys=False,
                 indent=None,
                 separators=None,
                 encoding='utf-8',
                 default=None):
        json.JSONEncoder.__init__(
            self, skipkeys=skipkeys,
            ensure_ascii=ensure_ascii,
            check_circular=check_circular,
            allow_nan=allow_nan,
            sort_keys=sort_keys,
            indent=indent,
            separators=separators,
            encoding=encoding,
            default=default
        )
        if self.default is not None:
            self.old_default = self.default

            def new_default(o):
                if isinstance(o, decimal.Decimal):
                    return str(o)
                return self.old_default(o)
        else:
            def new_default(o):
                raise TypeError(repr(o) + " is not JSON serializable")

        self.default = new_default
