# Copyright 2015 Symantec Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import random
import socket
import time

from oslo.config import cfg

from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)

statsd_metrics_opts = [
    cfg.BoolOpt('enabled',
                default=False,
                help='enables StatsD metrics'),
    cfg.StrOpt('statsd_host',
               default="127.0.0.1",
               help='StatsD host'),
    cfg.IntOpt('statsd_port',
               default="8125",
               help='StatsD port'),
    cfg.StrOpt('statsd_sample_rate',
               default="1",
               help='StatsD sample rate'),
    cfg.StrOpt('host',
               default="",
               help='host name'),
    cfg.StrOpt('metric_name_prefix',
               default="",
               help='metrics name prefix'),

]

statsd_metrics_group = cfg.OptGroup(name='statsd_metrics',
                                    title='StatsD metrics options')

CONF = cfg.CONF
CONF.register_group(statsd_metrics_group)
CONF.register_opts(statsd_metrics_opts, group='statsd_metrics')

# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or (
        isinstance(value, basestring) and value.lower() in TRUE_VALUES)


class StatsdClient(object):
    """StatsClient is used to send metrics to a configured statsd server.
    """

    def __init__(self, enabled,
                 host, port,
                 prefix='',
                 metrics_field_separator_char='.',
                 default_sample_rate=1,
                 sample_rate_factor=1,
                 random_func=random.random):
        self.enabled = enabled
        self.host = host
        self.port = port
        self.target = (self.host, self.port)
        self.metrics_field_separator_char = metrics_field_separator_char

        if prefix:
            self.prefix = prefix + metrics_field_separator_char
        else:
            self.prefix = ''

        self.default_sample_rate = default_sample_rate
        self.sample_rate_factor = sample_rate_factor
        self.random = random_func

    @classmethod
    def from_config(cls, prefix=''):
        """Factory method to generate StatsdClient object.

        :param prefix: metric name prefix
        :return: StatsdClient object
        """
        enabled = config_true_value(CONF.statsd_metrics.enabled)
        statsd_host = CONF.statsd_metrics.statsd_host
        statsd_port = CONF.statsd_metrics.statsd_port
        statsd_sample_rate = float(CONF.statsd_metrics.statsd_sample_rate)

        if prefix:
            metric_name_prefix = prefix
        else:
            host = CONF.statsd_metrics.host or socket.gethostname()
            if CONF.statsd_metrics.metric_name_prefix:
                metric_name_prefix = ".".join(
                    [host, CONF.statsd_metrics.metric_name_prefix]
                )
            else:
                metric_name_prefix = host

        return StatsdClient(enabled,
                            statsd_host, statsd_port,
                            prefix=metric_name_prefix,
                            default_sample_rate=statsd_sample_rate,
                            sample_rate_factor=statsd_sample_rate)

    def _send(self, metrics_name, metrics_value, metrics_type, sample_rate):
        if not self.enabled:
            return

        if sample_rate is None:
            sample_rate = self.default_sample_rate
        sample_rate *= self.sample_rate_factor
        parts = ['%s%s:%s' % (self.prefix, metrics_name, metrics_value),
                 metrics_type]
        if sample_rate < 1:
            if self.random() < sample_rate:
                parts.append('@%s' % (sample_rate,))
            else:
                return

        with closing(self._open_socket()) as sock:
            try:
                statsd_event_msg = '|'.join(parts)
                LOG.debug(statsd_event_msg)
                return sock.sendto(statsd_event_msg, self.target)
            except IOError as err:
                LOG.warn(
                    'Error sending UDP message to %r: %s',
                    self.target, err)

    @staticmethod
    def _open_socket():
        return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def update_stats(self, m_name, m_value, sample_rate=None):
        return self._send(m_name, m_value, 'c', sample_rate)

    def increment(self, metric, n=1, sample_rate=None):
        return self.update_stats(metric, n, sample_rate)

    def decrement(self, metric, n=-1, sample_rate=None):
        return self.update_stats(metric, n, sample_rate)

    def timing(self, metric, timing_ms, sample_rate=None):
        return self._send(metric, timing_ms, 'ms', sample_rate)

    def timing_since(self, metric, orig_time, sample_rate=None):
        return self.timing(metric, (time.time() - orig_time) * 1000,
                           sample_rate)

    def gauge(self, metric, value, sample_rate=None):
        return self._send(metric, value, 'g', sample_rate)


class closing(object):
    """Context to automatically close something at the end of a block.

    Code like this:

        with closing(<module>.open(<arguments>)) as f:
            <block>

    is equivalent to this:

        f = <module>.open(<arguments>)
        try:
            <block>
        finally:
            f.close()

    """

    def __init__(self, thing):
        self.thing = thing

    def __enter__(self):
        return self.thing

    def __exit__(self, *exc_info):
        self.thing.close()
