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

import requests

from magnetodb.common.utils import statsdutil
from magnetodb.openstack.common import log as logging
from magnetodb.openstack.common import periodic_task
from magnetodb.openstack.common import service

LOG = logging.getLogger(__name__)


class HealthcheckMetricsService(service.Service):
    """Healthcheck metrics service to periodically send StatsD
     metrics
    """
    def __init__(
            self,
            healthcheck_url='http://127.0.0.1:8480/healthcheck?fullcheck=true',
            enabled=False,
            initial_delay=300,
            periodic_interval=60,
            auth_uri='http://127.0.0.1:5000/v3'):
        super(HealthcheckMetricsService, self).__init__()
        self.manager = HealthcheckMetricsManager(healthcheck_url, auth_uri)
        self.enabled = enabled
        self.initial_delay = initial_delay
        self.periodic_interval = periodic_interval

    def start(self):
        """Start HealthcheckMetricsService.

        :returns: None
        """
        if not self.enabled:
            return

        LOG.debug('Starting healthcheck metrics service')

        super(HealthcheckMetricsService, self).start()

        self.tg.add_dynamic_timer(self.manager.periodic_tasks,
                                  context=None,
                                  initial_delay=self.initial_delay,
                                  periodic_interval_max=self.periodic_interval)


class HealthcheckMetricsManager(periodic_task.PeriodicTasks):
    """Periodical task to invoke healthcheck API and send StatsD
     metrics
    """
    req_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Auth-Token': None
    }

    def __init__(self, healthcheck_url, auth_uri):
        super(HealthcheckMetricsManager, self).__init__()
        self.statsd_client = statsdutil.create_statsd_client_from_config()
        self.healthcheck_url = healthcheck_url
        self.auth_uri = auth_uri

    def periodic_tasks(self, context, raise_on_error=False):
        LOG.debug("Healthcheck task running")
        self._run_healthcheck()
        return self.run_periodic_tasks(context, raise_on_error=raise_on_error)

    def _run_healthcheck(self):
        try:
            resp = requests.get(self.healthcheck_url, headers=self.req_headers)
            overall_ok = True if resp.status_code == 200 else False

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
