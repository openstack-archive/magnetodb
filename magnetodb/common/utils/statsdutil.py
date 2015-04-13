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

import socket

from oslo_config import cfg

from magnetodb.openstack.common import log as logging
from magnetodb.statsd import statsd


LOG = logging.getLogger(__name__)

statsd_metrics_opts = [
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


def create_statsd_client_from_config(prefix=''):
    """Factory method to create StatsdClient object using configuration.

    :param prefix:
        metric name prefix to override the configured prefix metric_name_prefix
    :return: StatsdClient object
    """
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

    return statsd.StatsdClient(statsd_host, statsd_port,
                               prefix=metric_name_prefix,
                               default_sample_rate=statsd_sample_rate,
                               sample_rate_factor=statsd_sample_rate)
