# Copyright 2014 Symantec Corporation.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
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


__all__ = ['setup',
           'get_notifier',

           'EVENT_TYPE_REQUEST_TIMING',
           'EVENT_TYPE_REQUEST_TIMING_ERROR',
           'EVENT_TYPE_REQUEST_REQ_SIZE_BYTES',
           'EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR',
           'EVENT_TYPE_REQUEST_RSP_SIZE_BYTES',
           'EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR',

           'EVENT_TYPE_TABLE_CREATE',
           'EVENT_TYPE_TABLE_CREATE_ERROR',
           'EVENT_TYPE_TABLE_DELETE',
           'EVENT_TYPE_TABLE_DELETE_ERROR',

           'EVENT_TYPE_REQUEST_TABLE_CREATE',
           'EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR',
           'EVENT_TYPE_DYNAMO_TABLE_CREATE',
           'EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR',

           'EVENT_TYPE_REQUEST_TABLE_DELETE',
           'EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR',
           'EVENT_TYPE_DYNAMO_TABLE_DELETE',
           'EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR',

           'EVENT_TYPE_REQUEST_TABLE_DESCRIBE',
           'EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR',
           'EVENT_TYPE_DYNAMO_TABLE_DESCRIBE',
           'EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR',

           'EVENT_TYPE_REQUEST_TABLE_LIST',
           'EVENT_TYPE_REQUEST_TABLE_LIST_ERROR',
           'EVENT_TYPE_DYNAMO_TABLE_LIST',
           'EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR',

           'EVENT_TYPE_REQUEST_PUTITEM',
           'EVENT_TYPE_REQUEST_PUTITEM_ERROR',
           'EVENT_TYPE_DYNAMO_PUTITEM',
           'EVENT_TYPE_DYNAMO_PUTITEM_ERROR',

           'EVENT_TYPE_REQUEST_DELETEITEM',
           'EVENT_TYPE_REQUEST_DELETEITEM_ERROR',
           'EVENT_TYPE_DYNAMO_DELETEITEM',
           'EVENT_TYPE_DYNAMO_DELETEITEM_ERROR',

           'EVENT_TYPE_REQUEST_BATCHREAD',
           'EVENT_TYPE_REQUEST_BATCHREAD_ERROR',
           'EVENT_TYPE_DYNAMO_BATCHREAD',
           'EVENT_TYPE_DYNAMO_BATCHREAD_ERROR',

           'EVENT_TYPE_REQUEST_BATCHWRITE',
           'EVENT_TYPE_REQUEST_BATCHWRITE_ERROR',
           'EVENT_TYPE_DYNAMO_BATCHWRITE',
           'EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR',

           'EVENT_TYPE_REQUEST_UPDATEITEM',
           'EVENT_TYPE_REQUEST_UPDATEITEM_ERROR',
           'EVENT_TYPE_DYNAMO_UPDATEITEM',
           'EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR',

           'EVENT_TYPE_REQUEST_GETITEM',
           'EVENT_TYPE_REQUEST_GETITEM_ERROR',
           'EVENT_TYPE_DYNAMO_GETITEM',
           'EVENT_TYPE_DYNAMO_GETITEM_ERROR',

           'EVENT_TYPE_REQUEST_QUERY',
           'EVENT_TYPE_REQUEST_QUERY_ERROR',
           'EVENT_TYPE_DYNAMO_QUERY',
           'EVENT_TYPE_DYNAMO_QUERY_ERROR',

           'EVENT_TYPE_REQUEST_SCAN',
           'EVENT_TYPE_REQUEST_SCAN_ERROR',
           'EVENT_TYPE_DYNAMO_SCAN',
           'EVENT_TYPE_DYNAMO_SCAN_ERROR',
           'EVENT_TYPE_STREAMING_PATH_ERROR',
           'EVENT_TYPE_STREAMING_DATA_START',
           'EVENT_TYPE_STREAMING_DATA_END',
           'EVENT_TYPE_STREAMING_DATA_ERROR',
           'EVENT_TYPE_REQUEST_RATE_LIMITED']

import functools
import socket

from oslo.config import cfg
from oslo.messaging import notify
from oslo.messaging import serializer
from oslo.messaging import transport
from oslo_serialization import jsonutils

from magnetodb import common as mdb_common
from magnetodb import context as ctxt
from magnetodb.notifier import notifier

from magnetodb.notifier import notifier_util as nu


EVENT_TYPE_REQUEST_TIMING = nu.EVENT_TYPE_REQUEST_TIMING
EVENT_TYPE_REQUEST_TIMING_ERROR = nu.EVENT_TYPE_REQUEST_TIMING_ERROR
EVENT_TYPE_REQUEST_REQ_SIZE_BYTES = nu.EVENT_TYPE_REQUEST_REQ_SIZE_BYTES
EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR = (
    nu.EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR)
EVENT_TYPE_REQUEST_RSP_SIZE_BYTES = nu.EVENT_TYPE_REQUEST_RSP_SIZE_BYTES
EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR = (
    nu.EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR)

EVENT_TYPE_TABLE_CREATE = nu.EVENT_TYPE_TABLE_CREATE
EVENT_TYPE_TABLE_CREATE_ERROR = nu.EVENT_TYPE_TABLE_CREATE_ERROR
EVENT_TYPE_TABLE_DELETE = nu.EVENT_TYPE_TABLE_DELETE
EVENT_TYPE_TABLE_DELETE_ERROR = nu.EVENT_TYPE_TABLE_DELETE_ERROR

EVENT_TYPE_REQUEST_TABLE_CREATE = nu.EVENT_TYPE_REQUEST_TABLE_CREATE
EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR = (
    nu.EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR)
EVENT_TYPE_DYNAMO_TABLE_CREATE = nu.EVENT_TYPE_DYNAMO_TABLE_CREATE
EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR = nu.EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR

EVENT_TYPE_REQUEST_TABLE_DELETE = nu.EVENT_TYPE_REQUEST_TABLE_DELETE
EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR = (
    nu.EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR)
EVENT_TYPE_DYNAMO_TABLE_DELETE = nu.EVENT_TYPE_DYNAMO_TABLE_DELETE
EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR = nu.EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR

EVENT_TYPE_REQUEST_TABLE_DESCRIBE = nu.EVENT_TYPE_REQUEST_TABLE_DESCRIBE
EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR = (
    nu.EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR)
EVENT_TYPE_DYNAMO_TABLE_DESCRIBE = nu.EVENT_TYPE_DYNAMO_TABLE_DESCRIBE
EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR = (
    nu.EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR)

EVENT_TYPE_REQUEST_TABLE_LIST = nu.EVENT_TYPE_REQUEST_TABLE_LIST
EVENT_TYPE_REQUEST_TABLE_LIST_ERROR = nu.EVENT_TYPE_REQUEST_TABLE_LIST_ERROR
EVENT_TYPE_DYNAMO_TABLE_LIST = nu.EVENT_TYPE_DYNAMO_TABLE_LIST
EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR = nu.EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR

EVENT_TYPE_REQUEST_PUTITEM = nu.EVENT_TYPE_REQUEST_PUTITEM
EVENT_TYPE_REQUEST_PUTITEM_ERROR = nu.EVENT_TYPE_REQUEST_PUTITEM_ERROR
EVENT_TYPE_DYNAMO_PUTITEM = nu.EVENT_TYPE_DYNAMO_PUTITEM
EVENT_TYPE_DYNAMO_PUTITEM_ERROR = nu.EVENT_TYPE_DYNAMO_PUTITEM_ERROR

EVENT_TYPE_REQUEST_DELETEITEM = nu.EVENT_TYPE_REQUEST_DELETEITEM
EVENT_TYPE_REQUEST_DELETEITEM_ERROR = nu.EVENT_TYPE_REQUEST_DELETEITEM_ERROR
EVENT_TYPE_DYNAMO_DELETEITEM = nu.EVENT_TYPE_DYNAMO_DELETEITEM
EVENT_TYPE_DYNAMO_DELETEITEM_ERROR = nu.EVENT_TYPE_DYNAMO_DELETEITEM_ERROR

EVENT_TYPE_REQUEST_BATCHREAD = nu.EVENT_TYPE_REQUEST_BATCHREAD
EVENT_TYPE_REQUEST_BATCHREAD_ERROR = nu.EVENT_TYPE_REQUEST_BATCHREAD_ERROR
EVENT_TYPE_DYNAMO_BATCHREAD = nu.EVENT_TYPE_DYNAMO_BATCHREAD
EVENT_TYPE_DYNAMO_BATCHREAD_ERROR = nu.EVENT_TYPE_DYNAMO_BATCHREAD_ERROR

EVENT_TYPE_REQUEST_BATCHWRITE = nu.EVENT_TYPE_REQUEST_BATCHWRITE
EVENT_TYPE_REQUEST_BATCHWRITE_ERROR = nu.EVENT_TYPE_REQUEST_BATCHWRITE_ERROR
EVENT_TYPE_DYNAMO_BATCHWRITE = nu.EVENT_TYPE_DYNAMO_BATCHWRITE
EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR = nu.EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR

EVENT_TYPE_REQUEST_UPDATEITEM = nu.EVENT_TYPE_REQUEST_UPDATEITEM
EVENT_TYPE_REQUEST_UPDATEITEM_ERROR = nu.EVENT_TYPE_REQUEST_UPDATEITEM_ERROR
EVENT_TYPE_DYNAMO_UPDATEITEM = nu.EVENT_TYPE_DYNAMO_UPDATEITEM
EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR = nu.EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR

EVENT_TYPE_REQUEST_GETITEM = nu.EVENT_TYPE_REQUEST_GETITEM
EVENT_TYPE_REQUEST_GETITEM_ERROR = nu.EVENT_TYPE_REQUEST_GETITEM_ERROR
EVENT_TYPE_DYNAMO_GETITEM = nu.EVENT_TYPE_DYNAMO_GETITEM
EVENT_TYPE_DYNAMO_GETITEM_ERROR = nu.EVENT_TYPE_DYNAMO_GETITEM_ERROR

EVENT_TYPE_REQUEST_QUERY = nu.EVENT_TYPE_REQUEST_QUERY
EVENT_TYPE_REQUEST_QUERY_ERROR = nu.EVENT_TYPE_REQUEST_QUERY_ERROR
EVENT_TYPE_DYNAMO_QUERY = nu.EVENT_TYPE_DYNAMO_QUERY
EVENT_TYPE_DYNAMO_QUERY_ERROR = nu.EVENT_TYPE_DYNAMO_QUERY_ERROR

EVENT_TYPE_REQUEST_SCAN = nu.EVENT_TYPE_REQUEST_SCAN
EVENT_TYPE_REQUEST_SCAN_ERROR = nu.EVENT_TYPE_REQUEST_SCAN_ERROR
EVENT_TYPE_DYNAMO_SCAN = nu.EVENT_TYPE_DYNAMO_SCAN
EVENT_TYPE_DYNAMO_SCAN_ERROR = nu.EVENT_TYPE_DYNAMO_SCAN_ERROR

EVENT_TYPE_STREAMING_PATH_ERROR = nu.EVENT_TYPE_STREAMING_PATH_ERROR
EVENT_TYPE_STREAMING_DATA_START = nu.EVENT_TYPE_STREAMING_DATA_START
EVENT_TYPE_STREAMING_DATA_END = nu.EVENT_TYPE_STREAMING_DATA_END
EVENT_TYPE_STREAMING_DATA_ERROR = nu.EVENT_TYPE_STREAMING_DATA_ERROR

EVENT_TYPE_REQUEST_RATE_LIMITED = nu.EVENT_TYPE_REQUEST_RATE_LIMITED

extra_notifier_opts = [
    cfg.StrOpt('notification_service',
               default=mdb_common.PROJECT_NAME,
               help='Service publisher_id for outgoing notifications'),
    cfg.StrOpt('default_publisher_id',
               default=None,
               help='Default publisher_id for outgoing notifications'),

]

cfg.CONF.register_opts(extra_notifier_opts)

__NOTIFIER = None
__STATSD_ADAPTED_NOTIFIER = None


def setup():
    global __NOTIFIER
    assert __NOTIFIER is None
    global __STATSD_ADAPTED_NOTIFIER
    assert __STATSD_ADAPTED_NOTIFIER is None

    get_notifier()


def _get_messaging_notifier():
    global __NOTIFIER

    if not __NOTIFIER:
        service = cfg.CONF.notification_service
        host = cfg.CONF.default_publisher_id or socket.gethostname()
        publisher_id = '{}.{}'.format(service, host)
        __NOTIFIER = notify.Notifier(
            transport.get_transport(cfg.CONF),
            publisher_id,
            serializer=RequestContextSerializer(JsonPayloadSerializer())
        )

    return __NOTIFIER


def get_notifier():
    global __STATSD_ADAPTED_NOTIFIER

    if not __STATSD_ADAPTED_NOTIFIER:
        __STATSD_ADAPTED_NOTIFIER = (
            notifier.MagnetoDBNotifier(
                _get_messaging_notifier()))

    return __STATSD_ADAPTED_NOTIFIER


class JsonPayloadSerializer(serializer.NoOpSerializer):
    @staticmethod
    def serialize_entity(context, entity):
        return jsonutils.to_primitive(entity, convert_instances=True)


class RequestContextSerializer(serializer.Serializer):
    def __init__(self, base):
        self._base = base

    def serialize_entity(self, context, entity):
        if not self._base:
            return entity
        return self._base.serialize_entity(context, entity)

    def deserialize_entity(self, context, entity):
        if not self._base:
            return entity
        return self._base.deserialize_entity(context, entity)

    def serialize_context(self, context):
        return context.to_dict()

    def deserialize_context(self, context):
        return ctxt.RequestContext(**context)


def request_type(event, **dec_kwargs):
    """
    Returns a decorator that sets value for request.context.request_type in
    MagnetoDB API endpoint controllers. The request_type value will be used by
    Notifier to look up the corresponding event in Event_Registry.
    """

    def decorating_func(func):

        @functools.wraps(func)
        def _request_type(ctrl, req, *args, **kwargs):
            req.context.request_type = event
            resp = func(ctrl, req, *args, **kwargs)
            return resp

        return _request_type

    return decorating_func
