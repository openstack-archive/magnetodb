# Copyright 2015 Symantec Corporation
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
import collections

EVENT_TYPE_TABLE_CREATE_START = 'magnetodb.table.create.start',
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

STATSD_METRIC_CREATE_TABLE_TASK = "mdb.req.create_table_task"
STATSD_METRIC_CREATE_TABLE_ERROR_TASK = "mdb.req.create_table_task.error"
STATSD_METRIC_DELETE_TABLE_TASK = "mdb.req.delete_table_task"
STATSD_METRIC_DELETE_TABLE_ERROR_TASK = "mdb.req.delete_table_task.error"


class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError


DeliveryType = Enum(["messaging", "statsd", "messaging_statsd"])


StatsdMetricType = Enum(["TIMER", "COUNTER"])

messaging_delivery_types = [
    DeliveryType.messaging, DeliveryType.messaging_statsd
]

statsd_delivery_types = [
    DeliveryType.statsd, DeliveryType.messaging_statsd
]


# Event_Metric: namedtuple to define event type's StatsD metrics and
#               delivery mechanism
# metric: name of metrics
# type: notifier_statsd_adapter.StatsdMetricType.TIMER or
#       notifier_statsd_adapter.StatsdMetricType.COUNTER
# delivery: DeliveryType - can be messaging, or, statsd, or both
Event_Metric = collections.namedtuple('Event_Metric',
                                      ['metric', 'type', 'delivery'])


# Events is a dict that defines the event type, stats metric, and delivery
# mechanism. Key is event type. Value is Event_Metrics which defines StatsD
# metric and delivery mechanism.
#
# If value is None, it means event type's delivery mechanism is messaging only.
Events = {
    EVENT_TYPE_TABLE_CREATE_START: None,
    EVENT_TYPE_TABLE_CREATE_END:
        Event_Metric(STATSD_METRIC_CREATE_TABLE_TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_CREATE_ERROR:
        Event_Metric(STATSD_METRIC_CREATE_TABLE_ERROR_TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DELETE_START: None,
    EVENT_TYPE_TABLE_DELETE_END:
        Event_Metric(STATSD_METRIC_DELETE_TABLE_TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DELETE_ERROR:
        Event_Metric(STATSD_METRIC_DELETE_TABLE_ERROR_TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DESCRIBE: None,
    EVENT_TYPE_TABLE_LIST: None,
    EVENT_TYPE_DATA_PUTITEM: None,
    EVENT_TYPE_DATA_PUTITEM_START: None,
    EVENT_TYPE_DATA_PUTITEM_END: None,
    EVENT_TYPE_DATA_DELETEITEM: None,
    EVENT_TYPE_DATA_DELETEITEM_START: None,
    EVENT_TYPE_DATA_DELETEITEM_END: None,
    EVENT_TYPE_DATA_DELETEITEM_ERROR: None,
    EVENT_TYPE_DATA_BATCHWRITE_START: None,
    EVENT_TYPE_DATA_BATCHWRITE_END: None,
    EVENT_TYPE_DATA_BATCHREAD_START: None,
    EVENT_TYPE_DATA_BATCHREAD_END: None,
    EVENT_TYPE_DATA_UPDATEITEM: None,
    EVENT_TYPE_DATA_GETITEM: None,
    EVENT_TYPE_DATA_GETITEM_START: None,
    EVENT_TYPE_DATA_GETITEM_END: None,
    EVENT_TYPE_DATA_QUERY: None,
    EVENT_TYPE_DATA_QUERY_START: None,
    EVENT_TYPE_DATA_QUERY_END: None,
    EVENT_TYPE_DATA_SCAN_START: None,
    EVENT_TYPE_DATA_SCAN_END: None,
    EVENT_TYPE_STREAMING_PATH_ERROR: None,
    EVENT_TYPE_STREAMING_DATA_START: None,
    EVENT_TYPE_STREAMING_DATA_END: None,
    EVENT_TYPE_STREAMING_DATA_ERROR: None,
    EVENT_TYPE_REQUEST_RATE_LIMITED: None,
}
