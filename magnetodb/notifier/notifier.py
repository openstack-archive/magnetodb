# Copyright 2015 Symantec Corporation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import copy
from magnetodb.common.utils import request_context_decorator as req_ctxt
from magnetodb.notifier import notifier_util


class MagnetoDBNotifier(object):
    """Send notification messages.

    The MagnetoDBNotifier class is used for sending notification messages.
    Depending on the delivery mechanism specified in the event object, message
    will be delivered via the normal behaviors of the oslo Notifier object, or,
    other ways to deliver notification events/metrics in addition to or instead
    of the oslo Notifier mechanism.

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
    """

    VALUE = 'value'
    ERROR = 'error'
    RESPONSE_CONTENT_LENGTH = 'response_content_length'
    REQUEST_CONTENT_LENGTH = 'request_content_length'

    def __init__(self, notifier):
        """Construct a MagnetoDBNotifier object.
        :param notifier: an olso messaging notifier object
        :type notifier: notify.Notifier
        """
        self.notifier = notifier
        self._notify_actions = {}
        if self.notifier:
            self._notify_actions = {
                notifier_util.NotifyLevel.audit:
                    self.notifier.audit,
                notifier_util.NotifyLevel.critical:
                    self.notifier.critical,
                notifier_util.NotifyLevel.debug:
                    self.notifier.debug,
                notifier_util.NotifyLevel.error:
                    self.notifier.error,
                notifier_util.NotifyLevel.info:
                    self.notifier.info,
                notifier_util.NotifyLevel.sample:
                    self.notifier.sample,
                notifier_util.NotifyLevel.warn:
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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return

        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.audit(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.debug(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.info(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.warn(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.error(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.critical(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

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
        event_metric = self._get_event_metric(event_type)
        if not event_metric:
            # undefined event, skip
            return
        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            self.notifier.sample(ctxt, event_metric.event, payload)
        req_ctxt.clean_up_context(ctxt)

    def notify(self, ctxt, event_type, payload, aggregate_metrics=True):
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
        :param aggregate_metrics: whether to aggregate metrics for all tenants
        :type aggregate_metrics: bool
        :param payload: the notification payload
        :type payload: dict
        :raises: MessageDeliveryFailure
        """
        event_metric = self._get_event_metric(event_type,
                                              check_error_type=True)
        if not event_metric:
            # undefined event, skip
            return

        if (ctxt is not None and
                event_metric.delivery in
                notifier_util.messaging_delivery_types):
            notification_payload = copy.copy(payload)
            notification_payload.pop(self.VALUE)
            self._notify_actions[event_metric.notify_level](
                ctxt, event_type, notification_payload
            )
        req_ctxt.clean_up_context(ctxt)

    def _get_event_metric(self, event_type, check_error_type=False):
        """Look up in Event_Registry and return the Event_Metric object

        :param event_type:
        :param check_error_type:
        :return:Event_Metric object
        """
        event_metric = notifier_util.Event_Registry.get(event_type)
        if event_metric:
            return event_metric
        else:
            if not check_error_type:
                # undefined event
                return None
            return self._get_error_event_type(event_type)

    def _get_error_event_type(self, event_type):
        """Get event_type from Event_Registry if event type is an error
        event type based on event name.

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
            event_metric = notifier_util.Event_Registry.get(
                event_type[:-4])
            return event_metric

        # undefined event
        return None

    def _get_error_code(self, metric_name):
        """Get error code from an error metric_name.

        Error metric name has such pattern:
            [\w].error.[\d\d\d]

        :param metric_name: error metric name
        :return: empty string for non error metric_name,
                 otherwise the last 3 digits from the error metric_name
        """
        if not metric_name or len(metric_name) < 11:
            # invalid error metric_name
            return ''

        if (len(metric_name) > 10 and
                metric_name[-3:].isdigit() and
                metric_name[-10:-4] == "." + self.ERROR):
            return metric_name[-3:]

        return ''
