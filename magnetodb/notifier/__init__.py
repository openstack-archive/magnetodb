# Copyright 2014 Symantec Corporation.
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

import socket
from magnetodb.openstack.common.context import RequestContext

from oslo.config import cfg
from oslo.messaging import get_transport
from oslo.messaging import notify
from oslo.messaging import serializer


from magnetodb.common import PROJECT_NAME
from oslo.serialization import jsonutils

extra_notifier_opts = [
    cfg.StrOpt('notification_service',
               default=PROJECT_NAME,
               help='Service publisher_id for outgoing notifications'),
    cfg.StrOpt('default_publisher_id',
               default=None,
               help='Default publisher_id for outgoing notifications'),

]

cfg.CONF.register_opts(extra_notifier_opts)

EVENT_TYPE_TABLE_CREATE_START = 'magnetodb.table.create.start'
EVENT_TYPE_TABLE_CREATE_END = 'magnetodb.table.create.end'
EVENT_TYPE_TABLE_CREATE_ERROR = 'magnetodb.table.create.error'
EVENT_TYPE_TABLE_DELETE_START = 'magnetodb.table.delete.start'
EVENT_TYPE_TABLE_DELETE_END = 'magnetodb.table.delete.end'
EVENT_TYPE_TABLE_DELETE_ERROR = 'magnetodb.table.delete.error'
EVENT_TYPE_TABLE_DESCRIBE = 'magnetodb.table.describe'
EVENT_TYPE_TABLE_LIST = 'magnetodb.table.list'
EVENT_TYPE_DATA_PUTITEM = 'magnetodb.data.putitem'
EVENT_TYPE_DATA_PUTITEM_START = 'magnetodb.data.putitem.start'
EVENT_TYPE_DATA_PUTITEM_END = 'magnetodb.data.putitem.end'
EVENT_TYPE_DATA_DELETEITEM = 'magnetodb.data.deleteitem'
EVENT_TYPE_DATA_DELETEITEM_START = 'magnetodb.data.deleteitem.start'
EVENT_TYPE_DATA_DELETEITEM_END = 'magnetodb.data.deleteitem.end'
EVENT_TYPE_DATA_DELETEITEM_ERROR = 'magnetodb.data.deleteitem.error'
EVENT_TYPE_DATA_BATCHWRITE_START = 'magnetodb.data.batchwrite.start'
EVENT_TYPE_DATA_BATCHWRITE_END = 'magnetodb.data.batchwrite.end'
EVENT_TYPE_DATA_BATCHREAD_START = 'magnetodb.data.batchread.start'
EVENT_TYPE_DATA_BATCHREAD_END = 'magnetodb.data.batchread.end'
EVENT_TYPE_DATA_UPDATEITEM = 'magnetodb.data.updateitem'
EVENT_TYPE_DATA_GETITEM = 'magnetodb.data.getitem'
EVENT_TYPE_DATA_GETITEM_START = 'magnetodb.data.getitem.start'
EVENT_TYPE_DATA_GETITEM_END = 'magnetodb.data.getitem.end'
EVENT_TYPE_DATA_QUERY = 'magnetodb.data.query'
EVENT_TYPE_DATA_QUERY_START = 'magnetodb.data.query.start'
EVENT_TYPE_DATA_QUERY_END = 'magnetodb.data.query.end'
EVENT_TYPE_DATA_SCAN_START = 'magnetodb.data.scan.start'
EVENT_TYPE_DATA_SCAN_END = 'magnetodb.data.scan.end'
EVENT_TYPE_STREAMING_PATH_ERROR = 'magnetodb.streaming.path.error'
EVENT_TYPE_STREAMING_DATA_START = 'magnetodb.streaming.data.start'
EVENT_TYPE_STREAMING_DATA_END = 'magnetodb.streaming.data.end'
EVENT_TYPE_STREAMING_DATA_ERROR = 'magnetodb.streaming.data.error'
EVENT_TYPE_REQUEST_RATE_LIMITED = 'magnetodb.request.rate.limited'


__NOTIFIER = None


def setup():
    global __NOTIFIER
    assert __NOTIFIER is None

    get_notifier()


def get_notifier():
    global __NOTIFIER

    if not __NOTIFIER:
        service = cfg.CONF.notification_service
        host = cfg.CONF.default_publisher_id or socket.gethostname()
        publisher_id = '{}.{}'.format(service, host)

        __NOTIFIER = notify.Notifier(
            get_transport(cfg.CONF),
            publisher_id,
            serializer=RequestContextSerializer(JsonPayloadSerializer())
        )

    return __NOTIFIER


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
        return RequestContext(**context)
