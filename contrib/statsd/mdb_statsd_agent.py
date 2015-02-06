import statsd
import requests

from keystoneclient.v3 import client
from oslo.config import cfg

from openstack.common import service
from openstack.common import periodic_task
from openstack.common import log as logging

logging.setup(cfg.CONF, 'serv')

LOG = logging.getLogger(__name__)

cfg.CONF.register_opts(
    [cfg.StrOpt("config_file", default=None)]
)

cfg.CONF.backdoor_port = "3000:3050"
cfg.CONF.debug = True
cfg.CONF.verbose = True


class Service(service.Service, periodic_task.PeriodicTasks):
    def start(self):
        initial_delay = 1
        periodic_interval_max = 3
        self.tg.add_dynamic_timer(self.periodic_tasks,
                                  initial_delay=initial_delay,
                                  periodic_interval_max=periodic_interval_max)
        self._keystone = client.Client(
            username='demo', password='test',
            auth_url='http://172.16.0.2:5000/v3',
            user_domain_name='Default')
        self._statsd = statsd.StatsClient()
        self._mdb_mon_url = 'http://172.16.0.2:8480/v1/monitoring/projects'

    def periodic_tasks(self):
        try:
            self._send_stats()
        except Exception as ex:
            LOG.exception(ex)
        return self.run_periodic_tasks(None, None)

    def _send_stats(self):
        tenant_stats = {}
        for row in self._get_data():
            tenant_stats.setdefault(
                row['tenant'],
                {'active_count': 0,
                 'creating_count': 0,
                 'deleting_count': 0,
                 'create_failed_count': 0,
                 'delete_failed_count': 0,
                 'size': 0,
                 'item_count': 0}
            )
            if row['status'] == 'ACTIVE':
                self._statsd.gauge(
                    'mdb.tables.{tenant}.{name}.item_count'.format(**row),
                    row['usage_detailes']['item_count']
                )
                self._statsd.gauge(
                    'mdb.tables.{tenant}.{name}.size'.format(**row),
                    row['usage_detailes']['size']
                )
                tenant_stats[row['tenant']]['active_count'] += 1
                for stat, value in row['usage_detailes'].iteritems():
                    tenant_stats[row['tenant']][stat] += value
            elif row['status'] == 'CREATING':
                tenant_stats[row['tenant']]['creating_count'] += 1
            elif row['status'] == 'DELETING':
                tenant_stats[row['tenant']]['deleting_count'] += 1
            elif row['status'] == 'CREATE_FAILED':
                tenant_stats[row['tenant']]['create_failed_count'] += 1
            elif row['status'] == 'DELETE_FAILED':
                tenant_stats[row['tenant']]['delete_failed_count'] += 1

        for tenant, stats in tenant_stats.iteritems():
            for stat in stats:
                self._statsd.gauge('mdb.tables.{}.{}'.format(tenant, stat),
                                   stats[stat])

    def _get_data(self):
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Auth-Token': self._keystone.auth_token
        }
        return requests.get(self._mdb_mon_url, headers=headers).json()

if __name__ == '__main__':
    s = Service()
    l = service.launch(s, workers=1)
    l.wait()
