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

from magnetodb.common.utils import statsd


class Enum(set):
    def __getattr__(self, name):
        if name in self:
            return name
        raise AttributeError


StatsdMetricType = Enum(["TIMER", "COUNTER"])


class StatsdAdaptedNotifier(object):
    """Send notification messages.

    The StatsdAdaptedNotifier class is used for sending notification messages
    via the normal behaviors of the Notifier object, and at the mean time, send
    StatsD metrics via StatsdClient object.

    Notification messages follow the following format::

        {'message_id': six.text_type(uuid.uuid4()),
         'publisher_id': 'myhost',
         'timestamp': timeutils.utcnow(),
         'priority': 'INFO',
         'event_type': 'magnetodb.table.create.end',
         'payload': {...},
         'metrics_name': 'mdb.req.create_table',
         'metrics_type': 'TIMER',
         'value': start_time
         }

    metrics_type is optional with a default metrics_type of TIMER.
    value is the start_time for TIMER metrics, and value to be incremented
    for COUNTER metrics and is optional if incremental value is 1.

    metrics_type, metrics_name, and value will be omitted when sending messages
    to oslo messaging notifier, but they are the only information used when
    sending metrics to StatsD.
    """

    def __init__(self, notifier):
        """Construct a StatsdAdaptedNotifier object.
        :param notifier: an olso messaging notifier object
        :type notifier: notify.Notifier
        """
        self.notifier = notifier
        self.statsd_client = statsd.StatsdClient.from_config()

    _marker = object()

    def prepare(self, publisher_id=_marker, retry=_marker):
        """Return a specialized StatsdAdaptedNotifier instance.

        Returns a new StatsdAdaptedNotifier instance with the supplied
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
        return StatsdAdaptedNotifier(new_notifier)

    def _send_statsd_metrics(self, metrics_name, metrics_type, value,
                             tenant_id=None):
        if not self.statsd_client.enabled:
            return

        if metrics_name:
            if metrics_type == StatsdMetricType.TIMER and value > 0:
                self.statsd_client.timing_since(metrics_name, value)
                if self.statsd_client.enabled_tenant and tenant_id:
                    metrics_name = '.'.join(
                        [metrics_name, tenant_id]
                    )
                    self.statsd_client.timing_since(metrics_name, value)

            elif metrics_type == StatsdMetricType.COUNTER:
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

    def audit(self, ctxt, event_type, payload,
              metrics_name=None,
              metrics_type=StatsdMetricType.TIMER,
              value=None):
        """Send a notification at audit level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.audit(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)

    def debug(self, ctxt, event_type, payload,
              metrics_name=None,
              metrics_type=StatsdMetricType.TIMER,
              value=None):
        """Send a notification at debug level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.debug(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)

    def info(self, ctxt, event_type, payload,
             metrics_name=None,
             metrics_type=StatsdMetricType.TIMER,
             value=None):

        """Send a notification at info level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.info(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)

    def warn(self, ctxt, event_type, payload,
             metrics_name=None,
             metrics_type=StatsdMetricType.TIMER,
             value=None):
        """Send a notification at warning level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.warn(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)

    def error(self, ctxt, event_type, payload,
              metrics_name=None,
              metrics_type=StatsdMetricType.TIMER,
              value=None):
        """Send a notification at error level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.error(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)

    def critical(self, ctxt, event_type, payload,
                 metrics_name=None,
                 metrics_type=StatsdMetricType.TIMER,
                 value=None):
        """Send a notification at critical level.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.critical(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)

    def sample(self, ctxt, event_type, payload,
               metrics_name=None,
               metrics_type=StatsdMetricType.TIMER,
               value=None):
        """Send a notification at sample level.

        Sample notifications are for high-frequency events
        that typically contain small payloads. eg: "CPU = 70%"

        Not all drivers support the sample level
        (log, for example) so these could be dropped.

        :param ctxt: a request context dict
        :type ctxt: dict
        :param event_type: describes the event, for example
                           'compute.create_instance'
        :type event_type: str
        :param payload: the notification payload
        :type payload: dict
        :param metrics_name: statsd metrics name
        :type metrics_name: str
        :param metrics_type: statsd metrics type
        :type metrics_type: StatsdMetricType
        :param value: for timing metrics, this is the start_time measured as
                      seconds since the Epoch. Fractions of a second may be
                      present if the system clock provides them.
                      for counter metrics, this is the counter value
        :type value: float or int
        :raises: MessageDeliveryFailure
        """
        self.notifier.sample(ctxt, event_type, payload)
        self._send_statsd_metrics(metrics_name, metrics_type, value,
                                  tenant_id=ctxt.tenant)
