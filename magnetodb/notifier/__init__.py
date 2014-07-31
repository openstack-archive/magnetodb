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
from oslo.config import cfg

from magnetodb.openstack.common.gettextutils import _
from magnetodb.openstack.common.notifier import api as notifier_api

from magnetodb.common import PROJECT_NAME

extra_notifier_opts = [
    cfg.StrOpt('notification_service',
               default=PROJECT_NAME,
               help='Service publisher_id for outgoing notifications')
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
EVENT_TYPE_DATA_SELECTITEM = 'magnetodb.data.selectitem'
EVENT_TYPE_DATA_SELECTITEM_START = 'magnetodb.data.selectitem.start'
EVENT_TYPE_DATA_SELECTITEM_END = 'magnetodb.data.selectitem.end'
EVENT_TYPE_DATA_SCAN_START = 'magnetodb.data.scan.start'
EVENT_TYPE_DATA_SCAN_END = 'magnetodb.data.scan.end'
EVENT_TYPE_STREAMING_PATH_ERROR = 'magnetodb.streaming.path.error'
EVENT_TYPE_STREAMING_DATA_START = 'magnetodb.streaming.data.start'
EVENT_TYPE_STREAMING_DATA_END = 'magnetodb.streaming.data.end'
EVENT_TYPE_STREAMING_DATA_ERROR = 'magnetodb.streaming.data.error'
EVENT_TYPE_REQUEST_RATE_LIMITED = 'magnetodb.request.rate.limited'

PRIORITY_DEBUG = notifier_api.DEBUG
PRIORITY_INFO = notifier_api.INFO
PRIORITY_WARN = notifier_api.WARN
PRIORITY_ERROR = notifier_api.ERROR
PRIORITY_CRITICAL = notifier_api.CRITICAL


__ALLOWED_EVENT_TYPES = (
    EVENT_TYPE_TABLE_CREATE_START,
    EVENT_TYPE_TABLE_CREATE_END,
    EVENT_TYPE_TABLE_CREATE_ERROR,
    EVENT_TYPE_TABLE_DELETE_START,
    EVENT_TYPE_TABLE_DELETE_END,
    EVENT_TYPE_TABLE_DELETE_ERROR,
    EVENT_TYPE_TABLE_DESCRIBE,
    EVENT_TYPE_TABLE_LIST,
    EVENT_TYPE_DATA_PUTITEM,
    EVENT_TYPE_DATA_PUTITEM_START,
    EVENT_TYPE_DATA_PUTITEM_END,
    EVENT_TYPE_DATA_DELETEITEM,
    EVENT_TYPE_DATA_DELETEITEM_START,
    EVENT_TYPE_DATA_DELETEITEM_END,
    EVENT_TYPE_DATA_DELETEITEM_ERROR,
    EVENT_TYPE_DATA_BATCHWRITE_START,
    EVENT_TYPE_DATA_BATCHWRITE_END,
    EVENT_TYPE_DATA_BATCHREAD_START,
    EVENT_TYPE_DATA_BATCHREAD_END,
    EVENT_TYPE_DATA_UPDATEITEM,
    EVENT_TYPE_DATA_SELECTITEM,
    EVENT_TYPE_DATA_SELECTITEM_START,
    EVENT_TYPE_DATA_SELECTITEM_END,
    EVENT_TYPE_DATA_SCAN_START,
    EVENT_TYPE_DATA_SCAN_END,
    EVENT_TYPE_STREAMING_PATH_ERROR,
    EVENT_TYPE_STREAMING_DATA_START,
    EVENT_TYPE_STREAMING_DATA_END,
    EVENT_TYPE_STREAMING_DATA_ERROR,
    EVENT_TYPE_REQUEST_RATE_LIMITED
)

__ALLOWED_PRIORITIES = (
    PRIORITY_DEBUG,
    PRIORITY_INFO,
    PRIORITY_WARN,
    PRIORITY_ERROR,
    PRIORITY_CRITICAL
)


__PUBLISHER_ID = None
__DEFAULT_PRIORITY = None


def setup():
    global __DEFAULT_PRIORITY
    global __PUBLISHER_ID

    assert __DEFAULT_PRIORITY is None
    assert __PUBLISHER_ID is None

    __DEFAULT_PRIORITY = cfg.CONF.default_notification_level

    __PUBLISHER_ID = notifier_api.publisher_id(
        cfg.CONF.notification_service
    )


class BadEventTypeException(Exception):
    pass


def notify(context, event_type, payload, priority=None):
    assert __PUBLISHER_ID is not None

    priority = priority or __DEFAULT_PRIORITY

    if event_type not in __ALLOWED_EVENT_TYPES:
        raise BadEventTypeException(
            _('%s is not a valid event type') % event_type)
    if priority not in __ALLOWED_PRIORITIES:
        raise notifier_api.BadPriorityException(
            _('%s not in valid priorities') % priority)
    notifier_api.notify(context, __PUBLISHER_ID, event_type, priority, payload)
