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

from magnetodb.common.utils import statsd
from magnetodb import context
from magnetodb.openstack.common import log as logging
from magnetodb.openstack.common import periodic_task
from magnetodb.openstack.common import service

from oslo.config import cfg

LOG = logging.getLogger(__name__)


hc_metrics_service_opts = [
    cfg.BoolOpt('enabled',
                default=False,
                help='enables health check metrics service'),
    cfg.IntOpt('initial_delay',
               default="300",
               help='initial delay'),
    cfg.IntOpt('periodic_interval',
               default="60",
               help='periodic interval'),
    cfg.StrOpt('auth_uri',
               default="http://127.0.0.1:5000/v3",
               help='keystone auth uri'),

]

hc_metrics_service_group = cfg.OptGroup(
    name='healthcheck_metrics_service',
    title='Health check metrics service options')

CONF = cfg.CONF
CONF.register_group(hc_metrics_service_group)
CONF.register_opts(hc_metrics_service_opts,
                   group='healthcheck_metrics_service')


class HealthcheckMetricsService(service.Service, periodic_task.PeriodicTasks):
    """Periodically invoke healthcheck on major components and send StatsD
     metrics
    """
    req_headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Auth-Token': None
    }

    def __init__(self, host='127.0.0.1', port=8480):
        super(HealthcheckMetricsService, self).__init__()
        self.statsd_client = statsd.StatsdClient.from_config()
        self.healthcheck_url = (
            'http://' + host + ':' + str(port) + '/healthcheck?fullcheck=true')
        self.auth_uri = CONF.healthcheck_metrics_service.auth_uri
        self.initial_delay = CONF.healthcheck_metrics_service.initial_delay
        self.periodic_interval = (
            CONF.healthcheck_metrics_service.periodic_interval)

    def start(self):
        """Start HealthcheckMetricsService.

        :returns: None
        """
        if not CONF.healthcheck_metrics_service.enabled:
            return

        LOG.debug('Starting healthcheck metrics service')

        super(HealthcheckMetricsService, self).start()

        context = self._generate_context()
        self.tg.add_dynamic_timer(self.periodic_tasks,
                                  initial_delay=self.initial_delay,
                                  periodic_interval_max=self.periodic_interval,
                                  context=context)

    def periodic_tasks(self, context, raise_on_error=False):
        LOG.debug(
            "Healthcheck tasks running with context %s" % context.to_dict())
        self._run_healthcheck(context)
        return self.run_periodic_tasks(context, raise_on_error=raise_on_error)

    def _generate_context(self):
        """Create a context.
        """
        tenant_name = None
        auth_token = None
        user_id = None
        service_catalog = None

        is_admin = False
        roles = None

        return context.RequestContext(auth_token=auth_token,
                                      user=user_id,
                                      tenant=tenant_name,
                                      is_admin=is_admin,
                                      roles=roles,
                                      service_catalog=service_catalog)

    def _run_healthcheck(self, context):
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
