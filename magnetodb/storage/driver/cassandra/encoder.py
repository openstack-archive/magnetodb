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

import binascii
import collections

from cassandra import encoder
from oslo_serialization import jsonutils as json


def _encode_b(value):
    return "0x" + binascii.hexlify(value)


def _encode_ss(value):
    return "{{{}}}".format(','.join(map(encoder.cql_quote, value)))


def _encode_ns(value):
    return "{{{}}}".format(','.join(map(str, value)))


def _encode_bs(value):
    return "{{{}}}".format(','.join(map(_encode_b, value)))


def _encode_ssm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, encoder.cql_quote(k), ":",
                        encoder.cql_quote(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_snm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, encoder.cql_quote(k), ":", str(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_sbm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, encoder.cql_quote(k), ":", _encode_b(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_nsm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, str(k), ":", encoder.cql_quote(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_nnm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, str(k), ":", str(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_nbm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, str(k), ":", _encode_b(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_bsm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, _encode_b(k), ":", encoder.cql_quote(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_bnm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, _encode_b(k), ":", str(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


def _encode_bbm(value):
    builder = collections.deque()
    prefix = '{'
    for k, v in value.iteritems():
        builder.extend((prefix, _encode_b(k), ":", _encode_b(v)))
        prefix = ","
    builder.append('}')
    return "".join(builder)


_CQL_ENCODER_MAP = {
    'S': encoder.cql_quote,
    'N': str,
    'B': _encode_b,
    'SS': _encode_ss,
    'NS': _encode_ns,
    'BS': _encode_bs,
    'SSM': _encode_ssm,
    'SNM': _encode_snm,
    'SBM': _encode_sbm,
    'NSM': _encode_nsm,
    'NNM': _encode_nnm,
    'NBM': _encode_nbm,
    'BSM': _encode_bsm,
    'BNM': _encode_bnm,
    'BBM': _encode_bbm
}


def encode_predefined_attr_value(attr_value):
    if attr_value is None:
        return 'null'

    return (
        _CQL_ENCODER_MAP[attr_value.attr_type.type](attr_value.decoded_value)
    )


def encode_dynamic_attr_value(attr_value):
    if attr_value is None:
        return 'null'

    return "0x" + binascii.hexlify(
        json.dumps(attr_value.encoded_value, sort_keys=True)
    )
