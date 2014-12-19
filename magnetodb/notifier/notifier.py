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

import copy
from magnetodb.common.utils import statsd
from magnetodb.notifier import notifier_statsd_util


class MagnetoDBNotifier(object):
    """Send notification messages.

    The MagnetoDBNotifier class is used for sending notification messages.
    Depending on the delivery mechanism specified in the event object, message
    will be delivered via the normal behaviors of the oslo Notifier object, or,
    a corresponding StatsD metrics will be sent in addition to or instead of
    the Notifier mechanism.

    StatsD metrics is sent via StatsdClient object.

    Notification messages follow the following format::

        {'message_id': six.text_type(uuid.uuid4()),
         'publisher_id': 'myhost',
         'timestamp': timeutils.utcnow(),
         'priority': 'INFO',
         'event_type': 'magnetodb.table.create',
         'payload': {...},
         }

    The event name, delivery mechanism, metrics_type and metrics_name are
    looked up in the event registry using event_type as the key.

    'value' should exist in payload dict when statsd metrics is used.
    For timing metrics, this is the start_time measured as seconds since the
    Epoch. Fractions of a second may be present if the system clock provides
    them. For counter metrics, this is the counter value of type float or int.
    """

    VALUE = "value"
    ERROR = "error"

    def __init__(self, notifier):
        """Construct a MagnetoDBNotifier object.
        :param notifier: an olso messaging notifier object
        :type notifier: notify.Notifier
        """
        self.notifier = notifier
        self.notify_actions = {}
        self.statsd_client = statsd.StatsdClient.from_config()
        if self.notifier:
            self.notify_actions = {
                notifier_statsd_util.NotifyLevel.audit:
                    self.notifier.audit,
                notifier_statsd_util.NotifyLevel.critical:
                    self.notifier.critical,
                notifier_statsd_util.NotifyLevel.debug:
                    self.notifier.debug,
                notifier_statsd_util.NotifyLevel.error:
                    self.notifier.error,
                notifier_statsd_util.NotifyLevel.info:
                    self.notifier.info,
                notifier_statsd_util.NotifyLevel.sample:
                    self.notifier.sample,
                notifier_statsd_util.NotifyLevel.warn:
                    self.notifier.warn
            }

    _marker = object()

    def prepare(self, publisher_id=_marker, retry=_marker):
        """Return a specialized MagnetoDBNotifier instance.

        Returns a new MagnetoDBNotifier instance with the supplied
        publisher_id. Allows sending notifications from multiple publisher_ids
        without the overhead of oslo messaging notification driver loading.

        :param publisher_id: field in notifications sent, for example
                             'compute.host1'
        :type publisher_id: str
        :param retry: an connection retries configuration
                      None or -1 means to retry forever
                      0 means no retry
                      N means N retries
        :type retry: int
        """
        new_notifier = self.notifier.prepare(publisher_id, retry=retry)
        return MagnetoDBNotifier(new_notifier)

    def audit(self, ctxt, event_type, payload):
        """Send a notification at audit level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return

        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.audit(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def debug(self, ctxt, event_type, payload):
        """Send a notification at debug level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.debug(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def info(self, ctxt, event_type, payload):

        """Send a notification at info level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.info(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def warn(self, ctxt, event_type, payload):
        """Send a notification at warning level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.warn(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def error(self, ctxt, event_type, payload):
        """Send a notification at error level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.error(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def critical(self, ctxt, event_type, payload):
        """Send a notification at critical level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.critical(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def sample(self, ctxt, event_type, payload):
        """Send a notification at sample level.

        Sample notifications are for high-frequency events
        that typically contain small payloads. eg: "CPU = 70%"

        Not all drivers support the sample level
        (log, for example) so these could be dropped.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            self.notifier.sample(ctxt, event_metric.event, payload)
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def notify(self, ctxt, event_type, payload):
        """Send a notification event.

        Notification level is determined by looking up the Event_Registry class
        using event_type. It's a no-op for unrecognized event type.

        User can use any one of the above API methods:
            audit, debug, info, warn, error, critical, sample
        Or, user can use this API method to let the notifier decide what
        notification level to use.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'magnetodb.table.create'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self.__get_event_metric(event_type,
                                               check_error_type=True)
        if not event_metric:
            # undefined event, skip
            return

        if (event_metric.delivery in
                notifier_statsd_util.messaging_delivery_types):
            notification_payload = copy.copy(payload)
            notification_payload.pop(self.VALUE)
            self.notify_actions[event_metric.notify_level](
                ctxt, event_type, notification_payload
            )
        self.__deliver_statsd_metrics(ctxt, event_metric, payload)
        self.__clean_up_context(ctxt)

    def __send_statsd_metrics(self, metrics_name, metrics_type, value,
                              tenant_id=None):
        """Send metrics to StatsD.

        :param metrics_name: StatsD metrics name
        :type metrics_name: str
        :param metrics_type: StatsD metrics type: timer or counter
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :param tenant_id: tenant id
        :type tenant_id: str
        :raises: MessageDeliveryFailure
        """
        if not self.statsd_client.enabled:
            return

        if metrics_name:
            if (metrics_type == notifier_statsd_util.StatsdMetricType.TIMER and
                    value > 0):
                self.statsd_client.timing_since(metrics_name, value)
                if self.statsd_client.enabled_tenant and tenant_id:
                    metrics_name = '.'.join(
                        [metrics_name, tenant_id]
                    )
                    self.statsd_client.timing_since(metrics_name, value)

            elif metrics_type == notifier_statsd_util.StatsdMetricType.COUNTER:
                if value:
                    self.statsd_client.increment(metrics_name, value)
                    if self.statsd_client.enabled_tenant and tenant_id:
                        metrics_name = '.'.join(
                            [metrics_name, tenant_id]
                        )
                        self.statsd_client.increment(metrics_name, value)
                else:
                    self.statsd_client.increment(metrics_name)
                    if self.statsd_client.enabled_tenant and tenant_id:
                        metrics_name = '.'.join(
                            [metrics_name, tenant_id]
                        )
                        self.statsd_client.increment(metrics_name)

    def __deliver_statsd_metrics(self, ctxt, event_metric, payload):
        """Conditionally deliver StatsD metrics depending on the event_metric
                definition and delivery method, and payload. Only when the
                following conditions are met, StatsD metrics will be sent:
                - event_metric has definition, and
                - its delivery attribute specifies it is for StatsD, and
                - payload is a dict, and
                - payload has 'value' as a key.
        :param ctxt: context
        :param event_metric: Event_Metric namedtuple
        :param payload:
        :return:
        """
        if (event_metric and
            (event_metric.delivery in
                notifier_statsd_util.statsd_delivery_types) and
            isinstance(payload, dict) and
                self.VALUE in payload):
            value = payload.get(self.VALUE)
            self.__send_statsd_metrics(event_metric.metric,
                                       event_metric.metric_type,
                                       value,
                                       tenant_id=ctxt.tenant)

    def __get_event_metric(self, event_type, check_error_type=False):
        event_metric = notifier_statsd_util.Event_Registry.get(event_type)
        if event_metric:
            return event_metric
        else:
            if not check_error_type:
                # undefined event
                return None
            if (len(event_type) > 10 and
                    event_type[-3:].isdigit() and
                    event_type[-10:-4] == "." + self.ERROR):
                event_metric = notifier_statsd_util.Event_Registry.get(
                    event_type[:-4])
                return event_metric
            return None

    def __is_event_error_type(self, event_type):
        """Check if even type is an error event type based on event name.

        Error type event name has such pattern:
            [\w].error.[\d\d\d]

        :param event_type:
        :return:
        """
        if not event_type or len(event_type) < 11:
            # undefined event
            return None
            if (len(event_type) > 10 and
                    event_type[-3:].isdigit() and
                    event_type[-10:-4] == "." + self.ERROR):
                event_metric = notifier_statsd_util.Event_Registry.get(
                    event_type[:-4])
                return event_metric

            # undefined event
            return None

    def __clean_up_context(self, ctxt):
        if hasattr(ctxt, 'message'):
            ctxt.__delattr__('message')
        if hasattr(ctxt, 'event'):
            ctxt.__delattr__('event')
