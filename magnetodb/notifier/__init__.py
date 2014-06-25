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
from magnetodb.common.utils.enum_item import EnumItem
from oslo.config import cfg

from magnetodb.openstack.common.notifier import api as notifier_api

from magnetodb.common import PROJECT_NAME

extra_notifier_opts = [
    cfg.StrOpt('notification_service',
               default=PROJECT_NAME,
               help='Service publisher_id for outgoing notifications')
]

cfg.CONF.register_opts(extra_notifier_opts)


class NotificationEventType(EnumItem):
    _allowed_item_ids = (
        'magnetodb.table.create.start',
        'magnetodb.table.create.end',
        'magnetodb.table.create.error',
        'magnetodb.table.delete.start',
        'magnetodb.table.delete.end',
        'magnetodb.table.delete.error',
        'magnetodb.table.describe',
        'magnetodb.table.list',
        'magnetodb.data.putitem',
        'magnetodb.data.putitem.start',
        'magnetodb.data.putitem.end',
        'magnetodb.data.deleteitem',
        'magnetodb.data.deleteitem.start',
        'magnetodb.data.deleteitem.end',
        'magnetodb.data.deleteitem.error',
        'magnetodb.data.batchwrite.start',
        'magnetodb.data.batchwrite.end',
        'magnetodb.data.batchread.start',
        'magnetodb.data.batchread.end',
        'magnetodb.data.updateitem',
        'magnetodb.data.selectitem',
        'magnetodb.data.selectitem.start',
        'magnetodb.data.selectitem.end',
        'magnetodb.data.scan.start',
        'magnetodb.data.scan.end'
    )

EVENT_TYPE_TABLE_CREATE_START = NotificationEventType(
    'magnetodb.table.create.start'
)
EVENT_TYPE_TABLE_CREATE_END = NotificationEventType(
    'magnetodb.table.create.end'
)
EVENT_TYPE_TABLE_CREATE_ERROR = NotificationEventType(
    'magnetodb.table.create.error'
)
EVENT_TYPE_TABLE_DELETE_START = NotificationEventType(
    'magnetodb.table.delete.start'
)
EVENT_TYPE_TABLE_DELETE_END = NotificationEventType(
    'magnetodb.table.delete.end'
)
EVENT_TYPE_TABLE_DELETE_ERROR = NotificationEventType(
    'magnetodb.table.delete.error'
)
EVENT_TYPE_TABLE_DESCRIBE = NotificationEventType(
    'magnetodb.table.describe'
)
EVENT_TYPE_TABLE_LIST = NotificationEventType(
    'magnetodb.table.list'
)
EVENT_TYPE_DATA_PUTITEM = NotificationEventType(
    'magnetodb.data.putitem'
)
EVENT_TYPE_DATA_PUTITEM_START = NotificationEventType(
    'magnetodb.data.putitem.start'
)
EVENT_TYPE_DATA_PUTITEM_END = NotificationEventType(
    'magnetodb.data.putitem.end'
)
EVENT_TYPE_DATA_DELETEITEM = NotificationEventType(
    'magnetodb.data.deleteitem'
)
EVENT_TYPE_DATA_DELETEITEM_START = NotificationEventType(
    'magnetodb.data.deleteitem.start'
)
EVENT_TYPE_DATA_DELETEITEM_END = NotificationEventType(
    'magnetodb.data.deleteitem.end'
)
EVENT_TYPE_DATA_DELETEITEM_ERROR = NotificationEventType(
    'magnetodb.data.deleteitem.error'
)
EVENT_TYPE_DATA_BATCHWRITE_START = NotificationEventType(
    'magnetodb.data.batchwrite.start'
)
EVENT_TYPE_DATA_BATCHWRITE_END = NotificationEventType(
    'magnetodb.data.batchwrite.end'
)
EVENT_TYPE_DATA_BATCHREAD_START = NotificationEventType(
    'magnetodb.data.batchread.start'
)
EVENT_TYPE_DATA_BATCHREAD_END = NotificationEventType(
    'magnetodb.data.batchread.end'
)
EVENT_TYPE_DATA_UPDATEITEM = NotificationEventType(
    'magnetodb.data.updateitem'
)
EVENT_TYPE_DATA_SELECTITEM = NotificationEventType(
    'magnetodb.data.selectitem'
)
EVENT_TYPE_DATA_SELECTITEM_START = NotificationEventType(
    'magnetodb.data.selectitem.start'
)
EVENT_TYPE_DATA_SELECTITEM_END = NotificationEventType(
    'magnetodb.data.selectitem.end'
)
EVENT_TYPE_DATA_SCAN_START = NotificationEventType(
    'magnetodb.data.scan.start'
)
EVENT_TYPE_DATA_SCAN_END = NotificationEventType(
    'magnetodb.data.scan.end'
)


class NotificationPriority(EnumItem):
    _allowed_item_ids = (
        notifier_api.DEBUG,
        notifier_api.INFO,
        notifier_api.WARN,
        notifier_api.ERROR,
        notifier_api.CRITICAL
    )


PRIORITY_DEBUG = NotificationPriority(notifier_api.DEBUG)
PRIORITY_INFO = NotificationPriority(notifier_api.INFO)
PRIORITY_WARN = NotificationPriority(notifier_api.WARN)
PRIORITY_ERROR = NotificationPriority(notifier_api.ERROR)
PRIORITY_CRITICAL = NotificationPriority(notifier_api.CRITICAL)


__PUBLISHER_ID = None
__DEFAULT_PRIORITY = None


def setup():
    global __DEFAULT_PRIORITY
    global __PUBLISHER_ID

    assert __DEFAULT_PRIORITY is None
    assert __PUBLISHER_ID is None

    __DEFAULT_PRIORITY = NotificationPriority(
        cfg.CONF.default_notification_level
    )

    __PUBLISHER_ID = notifier_api.publisher_id(
        cfg.CONF.notification_service
    )


def notify(context, event_type, payload, priority=None):
    assert __PUBLISHER_ID is not None

    notifier_api.notify(
        context, __PUBLISHER_ID, event_type.id,
        (priority or __DEFAULT_PRIORITY).id, payload
    )
