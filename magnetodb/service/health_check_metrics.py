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

import sys
import requests

from magnetodb.common.utils import statsdutil
from magnetodb.openstack.common import log as logging
from magnetodb.openstack.common import service
from oslo_config import cfg

LOG = logging.getLogger(__name__)


hc_metrics_service_opts = [
    cfg.BoolOpt('enabled',
                default=False,
                help='enables health check metrics service'),
    cfg.StrOpt('healthcheck_url',
               default="http://127.0.0.1:8480/healthcheck?fullcheck=true",
               help='health check url'),
    cfg.IntOpt('initial_delay',
               default="300",
               help='initial delay'),
    cfg.IntOpt('periodic_interval',
               default="60",
               help='periodic interval')
]

hc_metrics_service_group = cfg.OptGroup(
    name='healthcheck_metrics_service',
    title='Health check metrics service options'
)

CONF = cfg.CONF
CONF.register_group(hc_metrics_service_group)
CONF.register_opts(hc_metrics_service_opts,
                   group='healthcheck_metrics_service')


class HealthcheckMetricsService(service.Service):
    """Healthcheck metrics service to periodically send StatsD
     metrics
    """
    REQUEST_HEADERS = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Auth-Token': None
    }

    def __init__(self):
        super(HealthcheckMetricsService, self).__init__()

        self.healthcheck_url = CONF.healthcheck_metrics_service.healthcheck_url
        self.initial_delay = CONF.healthcheck_metrics_service.initial_delay
        self.periodic_interval = (
            CONF.healthcheck_metrics_service.periodic_interval)
        self.statsd_client = statsdutil.create_statsd_client_from_config()

    def start(self):
        """Start HealthcheckMetricsService.

        :returns: None
        """
        LOG.debug('Starting healthcheck metrics service')

        super(HealthcheckMetricsService, self).start()

        self.tg.add_dynamic_timer(self._run_healthcheck,
                                  initial_delay=self.initial_delay,
                                  periodic_interval_max=self.periodic_interval)

    def _run_healthcheck(self):
        LOG.debug("Healthcheck task running")

        try:
            resp = requests.get(self.healthcheck_url,
                                headers=self.REQUEST_HEADERS)
            overall_ok = resp.status_code == 200

            if resp.status_code not in (200, 503):
                LOG.error("error calling healthcheck: " + resp.text)
                api_ok = False
                identity_ok = False
                storage_ok = False
                messaging_ok = False
            else:
                # response body in the format of:
                #     {
                #         "Messaging": "ERROR",
                #         "API": "OK",
                #         "Storage": "OK",
                #         "Identity": "ERROR"
                #     }
                status = resp.json()
                LOG.debug(status)
                identity_ok = status["Identity"] == "OK"
                storage_ok = status["Storage"] == "OK"
                messaging_ok = status["Messaging"] == "OK"
                api_ok = status["API"] == "OK"

        except Exception:
            overall_ok = False
            api_ok = False
            identity_ok = False
            storage_ok = False
            messaging_ok = False

        self.statsd_client.gauge('magnetodb.health.all', overall_ok)
        self.statsd_client.gauge('magnetodb.health.api', api_ok)
        self.statsd_client.gauge('magnetodb.health.identity', identity_ok)
        self.statsd_client.gauge('magnetodb.health.storage', storage_ok)
        self.statsd_client.gauge('magnetodb.health.messaging', messaging_ok)

        return sys.maxint
