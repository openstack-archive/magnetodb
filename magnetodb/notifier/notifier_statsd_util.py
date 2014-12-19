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

ERROR_NAME = '.error'


class NotifyLevel:
    audit, debug, info, warn, error, critical, sample = xrange(7)


class DeliveryType:
    messaging, statsd, messaging_statsd = xrange(3)


class StatsdMetricType:
    TIMER, COUNTER = xrange(2)


messaging_delivery_types = [
    DeliveryType.messaging, DeliveryType.messaging_statsd
]

statsd_delivery_types = [
    DeliveryType.statsd, DeliveryType.messaging_statsd
]

# Event_Metric: namedtuple to define event type's StatsD metrics and
#               delivery mechanism
# event: name of messaging event
# metric: name of StatsD metric
# notify_level: messaging notification level, can be any value of NotifyLevel:
#               audit, debug, info, warn, error, critical, sample
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
                                       'metric_type',
                                       'delivery'])

EVENT_TYPE_TABLE_CREATE = 'magnetodb.table.create'
EVENT_TYPE_TABLE_CREATE_ERROR = EVENT_TYPE_TABLE_CREATE + ERROR_NAME

EVENT_TYPE_TABLE_DELETE = 'magnetodb.table.delete'
EVENT_TYPE_TABLE_DELETE_ERROR = EVENT_TYPE_TABLE_DELETE + ERROR_NAME

EVENT_TYPE_REQUEST_TABLE_CREATE = 'magnetodb.req.create_table'
EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR = (EVENT_TYPE_REQUEST_TABLE_CREATE
                                         + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_CREATE = 'magnetodb.dynamo.create_table'
EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR = (EVENT_TYPE_DYNAMO_TABLE_CREATE
                                        + ERROR_NAME)

EVENT_TYPE_REQUEST_TABLE_DELETE = 'magnetodb.req.delete_table'
EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR = (EVENT_TYPE_REQUEST_TABLE_DELETE
                                         + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_DELETE = 'magnetodb.dynamo.delete_table'
EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR = (EVENT_TYPE_DYNAMO_TABLE_DELETE
                                        + ERROR_NAME)

EVENT_TYPE_REQUEST_TABLE_DESCRIBE = 'magnetodb.req.describe_table'
EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR = (EVENT_TYPE_REQUEST_TABLE_DESCRIBE
                                           + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_DESCRIBE = 'magnetodb.dynamo.describe_table'
EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR = (EVENT_TYPE_DYNAMO_TABLE_DESCRIBE
                                          + ERROR_NAME)

EVENT_TYPE_REQUEST_TABLE_LIST = 'magnetodb.req.list_table'
EVENT_TYPE_REQUEST_TABLE_LIST_ERROR = (EVENT_TYPE_REQUEST_TABLE_LIST
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_TABLE_LIST = 'magnetodb.dynamo.list_table'
EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR = EVENT_TYPE_DYNAMO_TABLE_LIST + ERROR_NAME

EVENT_TYPE_REQUEST_PUTITEM = 'magnetodb.req.put_item'
EVENT_TYPE_REQUEST_PUTITEM_ERROR = EVENT_TYPE_REQUEST_PUTITEM + ERROR_NAME
EVENT_TYPE_DYNAMO_PUTITEM = 'magnetodb.dynamo.put_item'
EVENT_TYPE_DYNAMO_PUTITEM_ERROR = EVENT_TYPE_DYNAMO_PUTITEM + ERROR_NAME

EVENT_TYPE_REQUEST_DELETEITEM = 'magnetodb.req.delete_item'
EVENT_TYPE_REQUEST_DELETEITEM_ERROR = (EVENT_TYPE_REQUEST_DELETEITEM
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_DELETEITEM = 'magnetodb.dynamo.delete_item'
EVENT_TYPE_DYNAMO_DELETEITEM_ERROR = EVENT_TYPE_DYNAMO_DELETEITEM + ERROR_NAME

EVENT_TYPE_REQUEST_BATCHREAD = 'magnetodb.req.batch_read'
EVENT_TYPE_REQUEST_BATCHREAD_ERROR = EVENT_TYPE_REQUEST_BATCHREAD + ERROR_NAME
EVENT_TYPE_DYNAMO_BATCHREAD = 'magnetodb.dynamo.batch_read'
EVENT_TYPE_DYNAMO_BATCHREAD_ERROR = EVENT_TYPE_DYNAMO_BATCHREAD + ERROR_NAME

EVENT_TYPE_REQUEST_BATCHWRITE = 'magnetodb.req.batch_write'
EVENT_TYPE_REQUEST_BATCHWRITE_ERROR = (EVENT_TYPE_REQUEST_BATCHWRITE
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_BATCHWRITE = 'magnetodb.dynamo.batch_write'
EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR = EVENT_TYPE_DYNAMO_BATCHWRITE + ERROR_NAME

EVENT_TYPE_REQUEST_UPDATEITEM = 'magnetodb.req.update_item'
EVENT_TYPE_REQUEST_UPDATEITEM_ERROR = (EVENT_TYPE_REQUEST_UPDATEITEM
                                       + ERROR_NAME)
EVENT_TYPE_DYNAMO_UPDATEITEM = 'magnetodb.dynamo.update_item'
EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR = EVENT_TYPE_DYNAMO_UPDATEITEM + ERROR_NAME

EVENT_TYPE_REQUEST_GETITEM = 'magnetodb.req.get_item'
EVENT_TYPE_REQUEST_GETITEM_ERROR = EVENT_TYPE_REQUEST_GETITEM + ERROR_NAME
EVENT_TYPE_DYNAMO_GETITEM = 'magnetodb.dynamo.get_item'
EVENT_TYPE_DYNAMO_GETITEM_ERROR = EVENT_TYPE_DYNAMO_GETITEM + ERROR_NAME

EVENT_TYPE_REQUEST_QUERY = 'magnetodb.req.query'
EVENT_TYPE_REQUEST_QUERY_ERROR = EVENT_TYPE_REQUEST_QUERY + ERROR_NAME
EVENT_TYPE_DYNAMO_QUERY = 'magnetodb.dynamo.query'
EVENT_TYPE_DYNAMO_QUERY_ERROR = EVENT_TYPE_DYNAMO_QUERY + ERROR_NAME

EVENT_TYPE_REQUEST_SCAN = 'magnetodb.req.scan'
EVENT_TYPE_REQUEST_SCAN_ERROR = EVENT_TYPE_REQUEST_SCAN + ERROR_NAME
EVENT_TYPE_DYNAMO_SCAN = 'magnetodb.dynamo.scan'
EVENT_TYPE_DYNAMO_SCAN_ERROR = EVENT_TYPE_DYNAMO_SCAN + ERROR_NAME

EVENT_TYPE_STREAMING_PATH_ERROR = 'magnetodb.streaming.path.error'
EVENT_TYPE_STREAMING_DATA_START = 'magnetodb.streaming.data.start'
EVENT_TYPE_STREAMING_DATA_END = 'magnetodb.streaming.data.end'
EVENT_TYPE_STREAMING_DATA_ERROR = 'magnetodb.streaming.data.error'

EVENT_TYPE_REQUEST_RATE_LIMITED = 'magnetodb.request.rate.limited'

# Event_Registry is a dict that defines the event type, stats metric, and
# delivery mechanism. Key is event type. Value is Event_Metrics which defines
# StatsD metric and delivery mechanism.
#
Event_Registry = {
    EVENT_TYPE_TABLE_CREATE:
        Event_Metric(EVENT_TYPE_TABLE_CREATE,
                     NotifyLevel.audit,
                     'mdb.task.create_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_CREATE_ERROR:
        Event_Metric(EVENT_TYPE_TABLE_CREATE_ERROR,
                     NotifyLevel.error,
                     'mdb.task.create_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DELETE:
        Event_Metric(EVENT_TYPE_TABLE_DELETE,
                     NotifyLevel.audit,
                     'mdb.task.delete_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_TABLE_DELETE_ERROR:
        Event_Metric(EVENT_TYPE_TABLE_DELETE_ERROR,
                     NotifyLevel.error,
                     'mdb.task.delete_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_CREATE:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_CREATE,
                     NotifyLevel.info,
                     'mdb.req.create_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_CREATE_ERROR,
                     NotifyLevel.error,
                     'mdb.req.create_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_CREATE:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_CREATE,
                     NotifyLevel.info,
                     'mdb.dynamo.create_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_CREATE_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.create_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_DELETE:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DELETE,
                     NotifyLevel.info,
                     'mdb.req.delete_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DELETE_ERROR,
                     NotifyLevel.error,
                     'mdb.req.delete_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DELETE:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DELETE,
                     NotifyLevel.info,
                     'mdb.dynamo.delete_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DELETE_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.delete_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_DESCRIBE:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DESCRIBE,
                     NotifyLevel.debug,
                     'mdb.req.describe_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_DESCRIBE_ERROR,
                     NotifyLevel.error,
                     'mdb.req.describe_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DESCRIBE:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DESCRIBE,
                     NotifyLevel.debug,
                     'mdb.dynamo.describe_table',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_DESCRIBE_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.describe_table.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_TABLE_LIST:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_LIST,
                     NotifyLevel.debug,
                     'mdb.req.list_tables',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_TABLE_LIST_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_TABLE_LIST_ERROR,
                     NotifyLevel.error,
                     'mdb.req.list_tables.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_LIST:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_LIST,
                     NotifyLevel.debug,
                     'mdb.dynamo.list_tables',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_TABLE_LIST_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.list_tables.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_PUTITEM:
        Event_Metric(EVENT_TYPE_REQUEST_PUTITEM,
                     NotifyLevel.info,
                     'mdb.req.put_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_PUTITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_PUTITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.req.put_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_PUTITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_PUTITEM,
                     NotifyLevel.info,
                     'mdb.dynamo.put_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_PUTITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_PUTITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.put_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_DELETEITEM:
        Event_Metric(EVENT_TYPE_REQUEST_DELETEITEM,
                     NotifyLevel.info,
                     'mdb.req.delete_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_DELETEITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_DELETEITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.req.delete_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_DELETEITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_DELETEITEM,
                     NotifyLevel.info,
                     'mdb.dynamo.delete_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_DELETEITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_DELETEITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.delete_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_UPDATEITEM:
        Event_Metric(EVENT_TYPE_REQUEST_UPDATEITEM,
                     NotifyLevel.info,
                     'mdb.req.update_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_UPDATEITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_UPDATEITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.req.update_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_UPDATEITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_UPDATEITEM,
                     NotifyLevel.info,
                     'mdb.dynamo.update_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_UPDATEITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.update_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_GETITEM:
        Event_Metric(EVENT_TYPE_REQUEST_GETITEM,
                     NotifyLevel.info,
                     'mdb.req.get_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_GETITEM_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_GETITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.req.get_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_GETITEM:
        Event_Metric(EVENT_TYPE_DYNAMO_GETITEM,
                     NotifyLevel.info,
                     'mdb.dynamo.get_item',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_GETITEM_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_GETITEM_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.get_item.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_BATCHREAD:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHREAD,
                     NotifyLevel.info,
                     'mdb.req.batch_read',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_BATCHREAD_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHREAD_ERROR,
                     NotifyLevel.error,
                     'mdb.req.batch_read.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHREAD:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHREAD,
                     NotifyLevel.info,
                     'mdb.dynamo.batch_read',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHREAD_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHREAD_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.batch_read.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_BATCHWRITE:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHWRITE,
                     NotifyLevel.info,
                     'mdb.req.batch_write',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_BATCHWRITE_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_BATCHWRITE_ERROR,
                     NotifyLevel.error,
                     'mdb.req.batch_write.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHWRITE:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHWRITE,
                     NotifyLevel.info,
                     'mdb.dynamo.batch_write',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_BATCHWRITE_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.batch_write.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_QUERY:
        Event_Metric(EVENT_TYPE_REQUEST_QUERY,
                     NotifyLevel.info,
                     'mdb.req.query',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_QUERY_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_QUERY_ERROR,
                     NotifyLevel.error,
                     'mdb.req.query.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_QUERY:
        Event_Metric(EVENT_TYPE_DYNAMO_QUERY,
                     NotifyLevel.info,
                     'mdb.dynamo.query',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_QUERY_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_QUERY_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.query.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_REQUEST_SCAN:
        Event_Metric(EVENT_TYPE_REQUEST_SCAN,
                     NotifyLevel.info,
                     'mdb.req.scan',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_REQUEST_SCAN_ERROR:
        Event_Metric(EVENT_TYPE_REQUEST_SCAN_ERROR,
                     NotifyLevel.error,
                     'mdb.req.scan.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_SCAN:
        Event_Metric(EVENT_TYPE_DYNAMO_SCAN,
                     NotifyLevel.info,
                     'mdb.dynamo.scan',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),
    EVENT_TYPE_DYNAMO_SCAN_ERROR:
        Event_Metric(EVENT_TYPE_DYNAMO_SCAN_ERROR,
                     NotifyLevel.error,
                     'mdb.dynamo.scan.error',
                     StatsdMetricType.TIMER,
                     DeliveryType.messaging_statsd),

    EVENT_TYPE_STREAMING_PATH_ERROR:
        Event_Metric(EVENT_TYPE_STREAMING_PATH_ERROR,
                     NotifyLevel.error,
                     None,
                     None,
                     DeliveryType.messaging),
    EVENT_TYPE_STREAMING_DATA_START:
        Event_Metric(EVENT_TYPE_STREAMING_DATA_START,
                     NotifyLevel.info,
                     None,
                     None,
                     DeliveryType.messaging),
    EVENT_TYPE_STREAMING_DATA_END:
        Event_Metric(EVENT_TYPE_STREAMING_DATA_END,
                     NotifyLevel.info,
                     None,
                     None,
                     DeliveryType.messaging),
    EVENT_TYPE_STREAMING_DATA_ERROR:
        Event_Metric(EVENT_TYPE_STREAMING_DATA_ERROR,
                     NotifyLevel.error,
                     None,
                     None,
                     DeliveryType.messaging),

    EVENT_TYPE_REQUEST_RATE_LIMITED:
        Event_Metric(EVENT_TYPE_REQUEST_RATE_LIMITED,
                     NotifyLevel.info,
                     None,
                     None,
                     DeliveryType.messaging)
}
