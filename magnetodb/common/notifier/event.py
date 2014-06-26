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

from oslo.config import cfg

from magnetodb.openstack.common.gettextutils import _
from magnetodb.openstack.common.notifier.api import log_levels
from magnetodb.openstack.common.notifier.api import BadPriorityException
from magnetodb.openstack.common.notifier import api as notifier_api


class Notification(object):
    TABLE_CREATE_START = 'magnetodb.table.create.start'

    TABLE_CREATE_END = 'magnetodb.table.create.end'

    TABLE_CREATE_ERROR = 'magnetodb.table.create.error'

    TABLE_DELETE_START = 'magnetodb.table.delete.start'

    TABLE_DELETE_END = 'magnetodb.table.delete.end'

    TABLE_DELETE_ERROR = 'magnetodb.table.delete.error'

    TABLE_DESCRIBE = 'magnetodb.table.describe'

    TABLE_LIST = 'magnetodb.table.list'

    DATA_PUTITEM = 'magnetodb.data.putitem'

    DATA_PUTITEM_START = 'magnetodb.data.putitem.start'

    DATA_PUTITEM_END = 'magnetodb.data.putitem.end'

    DATA_DELETEITEM = 'magnetodb.data.deleteitem'

    DATA_DELETEITEM_START = 'magnetodb.data.deleteitem.start'

    DATA_DELETEITEM_END = 'magnetodb.data.deleteitem.end'

    DATA_DELETEITEM_ERROR = 'magnetodb.data.deleteitem.error'

    DATA_BATCHWRITE_START = 'magnetodb.data.batchwrite.start'

    DATA_BATCHWRITE_END = 'magnetodb.data.batchwrite.end'

    DATA_BATCHREAD_START = 'magnetodb.data.batchread.start'

    DATA_BATCHREAD_END = 'magnetodb.data.batchread.end'

    DATA_UPDATEITEM = 'magnetodb.data.updateitem'

    DATA_SELECTITEM = 'magnetodb.data.selectitem'

    DATA_SELECTITEM_START = 'magnetodb.data.selectitem.start'

    DATA_SELECTITEM_END = 'magnetodb.data.selectitem.end'

    DATA_SCAN_START = 'magnetodb.data.scan.start'

    DATA_SCAN_END = 'magnetodb.data.scan.end'

    STREAMING_PATH_ERROR = 'magnetodb.streaming.path.error'

    STREAMING_DATA_START = 'magnetodb.streaming.data.start'

    STREAMING_DATA_END = 'magnetodb.streaming.data.end'

    STREAMING_DATA_ERROR = 'magnetodb.streaming.data.error'

    event_types = (TABLE_CREATE_START,
                   TABLE_CREATE_END,
                   TABLE_CREATE_ERROR,
                   TABLE_DELETE_START,
                   TABLE_DELETE_END,
                   TABLE_DELETE_ERROR,
                   TABLE_DESCRIBE,
                   TABLE_LIST,
                   DATA_PUTITEM,
                   DATA_PUTITEM_START,
                   DATA_PUTITEM_END,
                   DATA_DELETEITEM,
                   DATA_DELETEITEM_START,
                   DATA_DELETEITEM_END,
                   DATA_DELETEITEM_ERROR,
                   DATA_BATCHWRITE_START,
                   DATA_BATCHWRITE_END,
                   DATA_BATCHREAD_START,
                   DATA_BATCHREAD_END,
                   DATA_UPDATEITEM,
                   DATA_SELECTITEM,
                   DATA_SELECTITEM_START,
                   DATA_SELECTITEM_END,
                   DATA_SCAN_START,
                   DATA_SCAN_END,
                   STREAMING_PATH_ERROR,
                   STREAMING_DATA_START,
                   STREAMING_DATA_END,
                   STREAMING_DATA_ERROR)

    WARN = notifier_api.WARN
    INFO = notifier_api.INFO
    ERROR = notifier_api.ERROR
    CRITICAL = notifier_api.CRITICAL
    DEBUG = notifier_api.DEBUG
    CONF = notifier_api.CONF

    def __init__(self):
        self.priority = notifier_api.CONF.default_notification_level
        self.publisher_id = notifier_api.publisher_id(
            cfg.CONF.notification_service
        )

    def __call__(self, context, event_type, payload,
                 priority=notifier_api.CONF.default_notification_level):
        if priority is None:
            priority = self.priority

        if event_type not in self.event_types:
            raise BadEventTypeException(
                _('%s is not a valid event type') % event_type)
        if priority not in log_levels:
            raise BadPriorityException(
                _('%s not in valid priorities') % priority)

        notifier_api.notify(context, self.publisher_id, event_type,
                            priority, payload)

notify = Notification()


class BadEventTypeException(Exception):
    pass
