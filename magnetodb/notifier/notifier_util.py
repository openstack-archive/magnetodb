# Copyright 2015 Symantec Corporation
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
import collections


ERROR_NAME = '.error'

# EVENT_TYPE_xxx should match what is set in context.request_type, which has
# the format of magnetodb.request.{service_name}.{action_name}, such as
# magnetodb.req.ddb.ListTables, or
# magnetodb.req.mdb.ListTables

EVENT_TYPE_REQUEST_TIMING = 'magnetodb.req.timing'
EVENT_TYPE_REQUEST_TIMING_ERROR = EVENT_TYPE_REQUEST_TIMING + ERROR_NAME

EVENT_TYPE_REQUEST_REQ_SIZE_BYTES = 'magnetodb.req.RequestSizeBytes'
EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR = (EVENT_TYPE_REQUEST_REQ_SIZE_BYTES +
                                           ERROR_NAME)

EVENT_TYPE_REQUEST_RSP_SIZE_BYTES = 'magnetodb.req.ResponseSizeBytes'
EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR = (EVENT_TYPE_REQUEST_RSP_SIZE_BYTES +
                                           ERROR_NAME)

EVENT_TYPE_TABLE_CREATE = 'magnetodb.table.create'
EVENT_TYPE_TABLE_CREATE_ERROR = EVENT_TYPE_TABLE_CREATE + ERROR_NAME

EVENT_TYPE_TABLE_DELETE = 'magnetodb.table.delete'
EVENT_TYPE_TABLE_DELETE_ERROR = EVENT_TYPE_TABLE_DELETE + ERROR_NAME

EVENT_TYPE_REQUEST_TABLE_CREATE = 'magnetodb.req.mdb.CreateTable'
EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR = (EVENT_TYPE_REQUEST_TABLE_CREATE
                                         + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_CREATE = 'magnetodb.req.ddb.CreateTable'
EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR = (EVENT_TYPE_DYNAMO_TABLE_CREATE
                                        + ERROR_NAME)

EVENT_TYPE_REQUEST_TABLE_DELETE = 'magnetodb.req.mdb.DeleteTable'
EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR = (EVENT_TYPE_REQUEST_TABLE_DELETE
                                         + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_DELETE = 'magnetodb.req.ddb.DeleteTable'
EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR = (EVENT_TYPE_DYNAMO_TABLE_DELETE
                                        + ERROR_NAME)

EVENT_TYPE_REQUEST_TABLE_DESCRIBE = 'magnetodb.req.mdb.DescribeTable'
EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR = (EVENT_TYPE_REQUEST_TABLE_DESCRIBE
                                           + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_DESCRIBE = 'magnetodb.req.ddb.DescribeTable'
EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR = (EVENT_TYPE_DYNAMO_TABLE_DESCRIBE
                                          + ERROR_NAME)

EVENT_TYPE_REQUEST_TABLE_LIST = 'magnetodb.req.mdb.ListTables'
EVENT_TYPE_REQUEST_TABLE_LIST_ERROR = (EVENT_TYPE_REQUEST_TABLE_LIST
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_LIST = 'magnetodb.req.ddb.ListTables'
EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR = EVENT_TYPE_DYNAMO_TABLE_LIST + ERROR_NAME

EVENT_TYPE_REQUEST_PUTITEM = 'magnetodb.req.mdb.PutItem'
EVENT_TYPE_REQUEST_PUTITEM_ERROR = EVENT_TYPE_REQUEST_PUTITEM + ERROR_NAME
EVENT_TYPE_DYNAMO_PUTITEM = 'magnetodb.req.ddb.PutItem'
EVENT_TYPE_DYNAMO_PUTITEM_ERROR = EVENT_TYPE_DYNAMO_PUTITEM + ERROR_NAME

EVENT_TYPE_REQUEST_DELETEITEM = 'magnetodb.req.mdb.DeleteItem'
EVENT_TYPE_REQUEST_DELETEITEM_ERROR = (EVENT_TYPE_REQUEST_DELETEITEM
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_DELETEITEM = 'magnetodb.req.ddb.DeleteItem'
EVENT_TYPE_DYNAMO_DELETEITEM_ERROR = EVENT_TYPE_DYNAMO_DELETEITEM + ERROR_NAME

EVENT_TYPE_REQUEST_BATCHREAD = 'magnetodb.req.mdb.BatchRead'
EVENT_TYPE_REQUEST_BATCHREAD_ERROR = EVENT_TYPE_REQUEST_BATCHREAD + ERROR_NAME
EVENT_TYPE_DYNAMO_BATCHREAD = 'magnetodb.req.ddb.BatchRead'
EVENT_TYPE_DYNAMO_BATCHREAD_ERROR = EVENT_TYPE_DYNAMO_BATCHREAD + ERROR_NAME

EVENT_TYPE_REQUEST_BATCHWRITE = 'magnetodb.req.mdb.BatchRrite'
EVENT_TYPE_REQUEST_BATCHWRITE_ERROR = (EVENT_TYPE_REQUEST_BATCHWRITE
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_BATCHWRITE = 'magnetodb.req.ddb.BatchWrite'
EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR = EVENT_TYPE_DYNAMO_BATCHWRITE + ERROR_NAME

EVENT_TYPE_REQUEST_UPDATEITEM = 'magnetodb.req.mdb.UpdateItem'
EVENT_TYPE_REQUEST_UPDATEITEM_ERROR = (EVENT_TYPE_REQUEST_UPDATEITEM
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_UPDATEITEM = 'magnetodb.req.ddb.UpdateItem'
EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR = EVENT_TYPE_DYNAMO_UPDATEITEM + ERROR_NAME

EVENT_TYPE_REQUEST_GETITEM = 'magnetodb.req.mdb.GetItem'
EVENT_TYPE_REQUEST_GETITEM_ERROR = EVENT_TYPE_REQUEST_GETITEM + ERROR_NAME
EVENT_TYPE_DYNAMO_GETITEM = 'magnetodb.req.ddb.GetItem'
EVENT_TYPE_DYNAMO_GETITEM_ERROR = EVENT_TYPE_DYNAMO_GETITEM + ERROR_NAME

EVENT_TYPE_REQUEST_QUERY = 'magnetodb.req.mdb.Query'
EVENT_TYPE_REQUEST_QUERY_ERROR = EVENT_TYPE_REQUEST_QUERY + ERROR_NAME
EVENT_TYPE_DYNAMO_QUERY = 'magnetodb.req.ddb.Query'
EVENT_TYPE_DYNAMO_QUERY_ERROR = EVENT_TYPE_DYNAMO_QUERY + ERROR_NAME

EVENT_TYPE_REQUEST_SCAN = 'magnetodb.req.mdb.Scan'
EVENT_TYPE_REQUEST_SCAN_ERROR = EVENT_TYPE_REQUEST_SCAN + ERROR_NAME
EVENT_TYPE_DYNAMO_SCAN = 'magnetodb.req.ddb.Scan'
EVENT_TYPE_DYNAMO_SCAN_ERROR = EVENT_TYPE_DYNAMO_SCAN + ERROR_NAME

EVENT_TYPE_STREAMING_PATH_ERROR = 'magnetodb.streaming.path.error'
EVENT_TYPE_STREAMING_DATA_START = 'magnetodb.streaming.data.start'
EVENT_TYPE_STREAMING_DATA_END = 'magnetodb.streaming.data.end'
EVENT_TYPE_STREAMING_DATA_ERROR = 'magnetodb.streaming.data.error'

EVENT_TYPE_REQUEST_RATE_LIMITED = 'magnetodb.request.rate.limited'


class NotifyLevel:
    audit, debug, info, warn, error, critical, sample = xrange(7)


class DeliveryType:
    messaging, statsd, messaging_statsd = xrange(3)


class StatsdMetricType:
    TIMER, COUNTER = xrange(2)


class StatsdMetricLevel:
    ALL_REQ, API, TASK = xrange(3)
    # ALL_REQ means StatsD metric is for all API request level, such as
    # magnetodb.req.timing
    # API means StatsD metric is for API endpoint request level, such as
    # magnetodb.req.mdb.GetItem
    # TASK means StatsD metric is for async internal task instead of API
    # request, such as magnetodb.table.create


messaging_delivery_types = [
    DeliveryType.messaging, DeliveryType.messaging_statsd
]


statsd_delivery_types = [
    DeliveryType.statsd, DeliveryType.messaging_statsd
]


# Event_Metric: namedtuple to define event type's StatsD metrics and
#               delivery mechanism
# event: name of messaging event
# notify_level: messaging notification level, can be any value of NotifyLevel:
#               audit, debug, info, warn, error, critical, sample
# metric: name of StatsD metric
# metric_level: StatsD metric level, can be any value of StatsdMetricLevel:
#               ALL_REQ, API, TASK
# metric_type: for StatsD metrics, the value can be
#              notifier_statsd_adapter.StatsdMetricType.TIMER or
#              notifier_statsd_adapter.StatsdMetricType.COUNTER
# delivery: DeliveryType - can be any value of DeliveryType:
#           DeliveryType.statsd,
#           DeliveryType.messaging_statsd,
#           DeliveryType.messaging
Event_Metric = collections.namedtuple('Event_Metric',
                                      ['event',
                                       'notify_level',
                                       'metric',
                                       'metric_level',
                                       'metric_type',
                                       'delivery'])


# Event_Registry is a dict that defines the event type, stats metric, and
# delivery mechanism. Key is event type. Value is Event_Metrics which defines
# StatsD metric and delivery mechanism.
#
Event_Registry = {
    EVENT_TYPE_REQUEST_TIMING:
        Event_Metric(None,
                     None,
                     EVENT_TYPE_REQUEST_TIMING,
                     StatsdMetricLevel.ALL_REQ,
                     StatsdMetricType.TIMER,
                     DeliveryType.statsd),
    EVENT_TYPE_REQUEST_TIMING_ERROR:
        Event_Metric(None,
                     None,
                     EVENT_TYPE_REQUEST_TIMING_ERROR,
                     StatsdMetricLevel.ALL_REQ,
                     StatsdMetricType.TIMER,
                     DeliveryType.statsd),
    EVENT_TYPE_REQUEST_REQ_SIZE_BYTES:
        Event_Metric(None,
                     None,
                     EVENT_TYPE_REQUEST_REQ_SIZE_BYTES,
                     StatsdMetricLevel.ALL_REQ,
                     StatsdMetricType.COUNTER,
                     DeliveryType.statsd),
    EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR:
        Event_Metric(None,
                     None,
                     EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR,
                     StatsdMetricLevel.ALL_REQ,
                     StatsdMetricType.COUNTER,
                     DeliveryType.statsd),
    EVENT_TYPE_REQUEST_RSP_SIZE_BYTES:
        Event_Metric(None,
                     None,
                     EVENT_TYPE_REQUEST_RSP_SIZE_BYTES,
                     StatsdMetricLevel.ALL_REQ,
                     StatsdMetricType.COUNTER,
                     DeliveryType.statsd),
    EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR:
        Event_Metric(None,
                     None,
                     EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR,
                     StatsdMetricLevel.ALL_REQ,
                     StatsdMetricType.COUNTER,
                     DeliveryType.statsd),
    EVENT_TYPE_TABLE_CREATE:
        Event_Metric(EVENT_TYPE_TABLE_CREATE,
                     NotifyLevel.audit,
                     'magnetodb.task.create_table',
                     StatsdMetricLevel.TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_CREATE_ERROR:
        Event_Metric(EVENT_TYPE_TABLE_CREATE_ERROR,
                     NotifyLevel.error,
                     'mdb.task.create_table.error',
                     StatsdMetricLevel.TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DELETE:
        Event_Metric(EVENT_TYPE_TABLE_DELETE,
                     NotifyLevel.audit,
                     'mdb.task.delete_table',
                     StatsdMetricLevel.TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DELETE_ERROR:
        Event_Metric(EVENT_TYPE_TABLE_DELETE_ERROR,
                     NotifyLevel.error,
                     'mdb.task.delete_table.error',
                     StatsdMetricLevel.TASK,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_CREATE:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_CREATE,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_TABLE_CREATE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_CREATE:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_CREATE,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_TABLE_CREATE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_DELETE:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DELETE,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_TABLE_DELETE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DELETE:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DELETE,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_TABLE_DELETE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_DESCRIBE:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DESCRIBE,
                     NotifyLevel.debug,
                     EVENT_TYPE_REQUEST_TABLE_DESCRIBE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DESCRIBE:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DESCRIBE,
                     NotifyLevel.debug,
                     EVENT_TYPE_DYNAMO_TABLE_DESCRIBE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_LIST:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_LIST,
                     NotifyLevel.debug,
                     EVENT_TYPE_REQUEST_TABLE_LIST,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_LIST_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_LIST_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_TABLE_LIST_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_LIST:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_LIST,
                     NotifyLevel.debug,
                     EVENT_TYPE_DYNAMO_TABLE_LIST,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_PUTITEM:
        Event_Metric(EVENT_TYPE_REQUEST_PUTITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_PUTITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_PUTITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_PUTITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_PUTITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_PUTITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_PUTITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_PUTITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_PUTITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_PUTITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_PUTITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_DELETEITEM:
        Event_Metric(EVENT_TYPE_REQUEST_DELETEITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_DELETEITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_DELETEITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_DELETEITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_DELETEITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_DELETEITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_DELETEITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_DELETEITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_DELETEITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_DELETEITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_DELETEITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_UPDATEITEM:
        Event_Metric(EVENT_TYPE_REQUEST_UPDATEITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_UPDATEITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_UPDATEITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_UPDATEITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_UPDATEITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_UPDATEITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_UPDATEITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_UPDATEITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_GETITEM:
        Event_Metric(EVENT_TYPE_REQUEST_GETITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_GETITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_GETITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_GETITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_GETITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_GETITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_GETITEM,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_GETITEM,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_GETITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_GETITEM_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_GETITEM_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_BATCHREAD:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHREAD,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_BATCHREAD,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_BATCHREAD_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHREAD_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_BATCHREAD_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHREAD:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHREAD,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_BATCHREAD,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHREAD_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHREAD_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_BATCHREAD_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_BATCHWRITE:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHWRITE,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_BATCHWRITE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_BATCHWRITE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHWRITE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_BATCHWRITE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHWRITE:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHWRITE,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_BATCHWRITE,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_QUERY:
        Event_Metric(EVENT_TYPE_REQUEST_QUERY,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_QUERY,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_QUERY_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_QUERY_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_QUERY_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_QUERY:
        Event_Metric(EVENT_TYPE_DYNAMO_QUERY,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_QUERY,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_QUERY_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_QUERY_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_QUERY_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_SCAN:
        Event_Metric(EVENT_TYPE_REQUEST_SCAN,
                     NotifyLevel.info,
                     EVENT_TYPE_REQUEST_SCAN,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_SCAN_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_SCAN_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_REQUEST_SCAN_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_SCAN:
        Event_Metric(EVENT_TYPE_DYNAMO_SCAN,
                     NotifyLevel.info,
                     EVENT_TYPE_DYNAMO_SCAN,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_SCAN_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_SCAN_ERROR,
                     NotifyLevel.error,
                     EVENT_TYPE_DYNAMO_SCAN_ERROR,
                     StatsdMetricLevel.API,
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_STREAMING_PATH_ERROR:
        Event_Metric(EVENT_TYPE_STREAMING_PATH_ERROR,
                     NotifyLevel.error,
                     None,
                     None,
                     None,
                     DeliveryType.messaging),
    EVENT_TYPE_STREAMING_DATA_START:
        Event_Metric(EVENT_TYPE_STREAMING_DATA_START,
                     NotifyLevel.info,
                     None,
                     None,
                     None,
                     DeliveryType.messaging),
    EVENT_TYPE_STREAMING_DATA_END:
        Event_Metric(EVENT_TYPE_STREAMING_DATA_END,
                     NotifyLevel.info,
                     None,
                     None,
                     None,
                     DeliveryType.messaging),
    EVENT_TYPE_STREAMING_DATA_ERROR:
        Event_Metric(EVENT_TYPE_STREAMING_DATA_ERROR,
                     NotifyLevel.error,
                     None,
                     None,
                     None,
                     DeliveryType.messaging),

    EVENT_TYPE_REQUEST_RATE_LIMITED:
        Event_Metric(EVENT_TYPE_REQUEST_RATE_LIMITED,
                     NotifyLevel.info,
                     None,
                     None,
                     None,
                     DeliveryType.messaging)
}
