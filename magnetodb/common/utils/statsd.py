# Copyright 2015 Symantec Corporation
# Copyright (c) 2010-2012 OpenStack Foundation
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
import functools
import sys

from oslo.config import cfg

from magnetodb.common import exception
from magnetodb.openstack.common import log as logging

RESP_SIZE_BYTES = "mdb.req.resp_size_bytes"
REQ_SIZE_BYTES = "mdb.req.size_bytes"

LOG = logging.getLogger(__name__)

req_metrics_opts = [
    cfg.BoolOpt('enabled',
                default=False,
                help='enables request metrics'),
    cfg.BoolOpt('enabled_apiendpoint',
                default=False,
                help='enables API endpoint level request metrics'),
    cfg.BoolOpt('enabled_tenant',
                default=False,
                help='enables tenant level request metrics'),
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

req_metrics_group = cfg.OptGroup(name='req_metrics',
                                 title='Request metrics options')

CONF = cfg.CONF
CONF.register_group(req_metrics_group)
CONF.register_opts(req_metrics_opts, group='req_metrics')

# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or (
        isinstance(value, basestring) and value.lower() in TRUE_VALUES)


class StatsdClient(object):
    """StatsClient is used to send request metrics to a configured statsd server.
    """

    def __init__(self, enabled, enabled_apiendpoint, enabled_tenant,
                 host, port,
                 prefix='',
                 metrics_field_separator_char='.',
                 default_sample_rate=1,
                 sample_rate_factor=1,
                 random_func=random.random):
        self.enabled = enabled
        self.enabled_apiendpoint = enabled_apiendpoint
        self.enabled_tenant = enabled_tenant
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
        enabled = config_true_value(CONF.req_metrics.enabled)
        enabled_apiendpoint = config_true_value(
            CONF.req_metrics.enabled_apiendpoint)
        enabled_tenant = config_true_value(CONF.req_metrics.enabled_tenant)
        statsd_host = CONF.req_metrics.statsd_host
        statsd_port = CONF.req_metrics.statsd_port
        statsd_sample_rate = float(CONF.req_metrics.statsd_sample_rate)

        if prefix:
            metric_name_prefix = prefix
        else:
            host = CONF.req_metrics.host or socket.gethostname()
            if CONF.req_metrics.metric_name_prefix:
                metric_name_prefix = ".".join(
                    [host, CONF.req_metrics.metric_name_prefix]
                )
            else:
                metric_name_prefix = host

        return StatsdClient(enabled, enabled_apiendpoint, enabled_tenant,
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


class Statsd(object):
    """Statsd holds an StatsdClient object.

    Usage example:
        @Statsd
        def query(req, body, project_id, table_name):
            ...

    """

    def __init__(self, prefix=""):
        self.statsd_client = StatsdClient.from_config()

    def __call__(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            if not hasattr(f, "statsd_client"):
                f.statsd_client = self.statsd_client
            return f(*args, **kwargs)

        return wrapper


def exception_2_error_code(error):
    """Translate exception to HTTP response error code

    :param error: Exception
    :return: HTTP error code
    """
    if error is exception.Forbidden:
        return 403
    elif (error is exception.InvalidQueryParameter
          or error is exception.ValidationError):
        return 400
    else:
        return 500


def timer_stats(metric_name, **dec_kwargs):
    """Returns a decorator that sends timing events or errors for MagnetoDB API
    endpoint controllers, based on response status code.

    Usage example:
        @statsd.timer_stats("mdb.req.delete_table")
        def delete_table(self, req, project_id, table_name):
            ...

    """

    def decorating_func(func):

        @functools.wraps(func)
        def _timing_stats(ctrl, *args, **kwargs):
            start_time = time.time()
            if not (hasattr(ctrl, "statsd_client") and ctrl.statsd_client):
                ctrl.statsd_client = StatsdClient.from_config()
            try:
                resp = func(ctrl, *args, **kwargs)
                if (ctrl.statsd_client.enabled and
                        ctrl.statsd_client.enabled_apiendpoint):
                    ctrl.statsd_client.timing_since(metric_name,
                                                    start_time,
                                                    **dec_kwargs)
                    if ctrl.statsd_client.enabled_tenant:
                        tenant_id = kwargs["project_id"]
                        ctrl.statsd_client.timing_since(metric_name + '.'
                                                        + tenant_id,
                                                        start_time,
                                                        **dec_kwargs)
                return resp
            except Exception as ex:
                error_code = exception_2_error_code(ex)
                if (ctrl.statsd_client.enabled and
                        ctrl.statsd_client.enabled_apiendpoint):
                    ctrl.statsd_client.timing_since((metric_name +
                                                     '.error.' +
                                                     str(error_code)),
                                                    start_time,
                                                    **dec_kwargs)
                    if ctrl.statsd_client.enabled_tenant:
                        tenant_id = kwargs["project_id"]
                        ctrl.statsd_client.timing_since((metric_name +
                                                         '.error.' +
                                                         str(error_code) +
                                                         '.' + tenant_id),
                                                        start_time,
                                                        **dec_kwargs)
                raise

        return _timing_stats

    return decorating_func


def counter_stats(api_metric_name, **dec_kwargs):
    """Returns a decorator that sends counter events or errors for MagnetoDB API
    endpoint controllers, based on response status code.

    Usage example:
        @statsd.counter_stats("mdb.req.xxxx")
        def xxxx(self, req, project_id, table_name):
            ...

    """
    def decorating_func(func):

        @functools.wraps(func)
        def _count_stats(ctrl, *args, **kwargs):
            if not getattr(ctrl, "statsd_client"):
                ctrl.statsd_client = StatsdClient.from_config()

            req_size = 0
            if kwargs["body"]:
                req_size = sys.getsizeof(kwargs["body"])

            try:
                resp = func(ctrl, *args, **kwargs)

                if (ctrl.statsd_client.enabled and
                        ctrl.statsd_client.enabled_apiendpoint):
                    req_metric_name = REQ_SIZE_BYTES + "." + api_metric_name
                    resp_metric_name = (RESP_SIZE_BYTES + "." +
                                        api_metric_name)
                    resp_size = sys.getsizeof(resp)

                    ctrl.statsd_client.increment(req_metric_name,
                                                 n=req_size,
                                                 **dec_kwargs)
                    ctrl.statsd_client.increment(resp_metric_name,
                                                 n=resp_size,
                                                 **dec_kwargs)

                    if ctrl.statsd_client.enabled_tenant:
                        tenant_id = kwargs["project_id"]
                        req_tenant_metric_name = (
                            REQ_SIZE_BYTES + "." +
                            api_metric_name + "." +
                            tenant_id
                        )
                        resp_tenant_metric_name = (RESP_SIZE_BYTES + "." +
                                                   api_metric_name + "." +
                                                   tenant_id)

                        ctrl.statsd_client.increment(
                            req_tenant_metric_name + "." + tenant_id,
                            n=req_size,
                            **dec_kwargs)
                        ctrl.statsd_client.increment(
                            resp_tenant_metric_name + "." + tenant_id,
                            n=resp_size,
                            **dec_kwargs)

                return resp
            except exception.MagnetoError as ex:
                error_code = exception_2_error_code(ex)
                if (ctrl.statsd_client.enabled and
                        ctrl.statsd_client.enabled_apiendpoint):
                    req_metric_name = (REQ_SIZE_BYTES +
                                       ".error." +
                                       str(error_code) +
                                       api_metric_name)
                    resp_metric_name = (RESP_SIZE_BYTES +
                                        ".error." +
                                        str(error_code) +
                                        api_metric_name)
                    resp_size = sys.getsizeof(ex.message)
                    ctrl.statsd_client.increment(req_metric_name,
                                                 n=req_size, **dec_kwargs)
                    ctrl.statsd_client.increment(resp_metric_name,
                                                 n=resp_size, **dec_kwargs)
                    if ctrl.statsd_client.enabled_tenant:
                        tenant_id = kwargs["project_id"]
                        req_tenant_metric_name = (
                            REQ_SIZE_BYTES + ".error." +
                            str(error_code) + api_metric_name +
                            "." +
                            tenant_id
                        )
                        resp_tenant_metric_name = (
                            RESP_SIZE_BYTES + ".error." +
                            str(error_code) + api_metric_name +
                            "." +
                            tenant_id
                        )

                        ctrl.statsd_client.increment(
                            req_tenant_metric_name + "." +
                            tenant_id, n=req_size, **dec_kwargs)
                        ctrl.statsd_client.increment(
                            resp_tenant_metric_name + "." +
                            tenant_id, n=resp_size, **dec_kwargs)
                raise

        return _count_stats

    return decorating_func


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
