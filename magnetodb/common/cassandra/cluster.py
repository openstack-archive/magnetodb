from concurrent.futures import ThreadPoolExecutor

from threading import Lock, RLock

import weakref
from weakref import WeakValueDictionary
try:
    from weakref import WeakSet
except ImportError:
    from cassandra.util import WeakSet  # NOQA

from cassandra import cluster
from cassandra.metadata import Metadata
from cassandra.metrics import Metrics
from cassandra.policies import (RoundRobinPolicy, HostDistance)


class Cluster(cluster.Cluster):
    def __init__(self,
                 contact_points=("127.0.0.1",),
                 port=9042,
                 compression=True,
                 auth_provider=None,
                 load_balancing_policy=None,
                 reconnection_policy=None,
                 default_retry_policy=None,
                 conviction_policy_factory=None,
                 metrics_enabled=False,
                 connection_class=None,
                 ssl_options=None,
                 sockopts=None,
                 cql_version=None,
                 executor_threads=2,
                 max_schema_agreement_wait=10):
        """
        Any of the mutable Cluster attributes may be set as keyword arguments
        to the constructor.
        """

        self.contact_points = contact_points
        self.port = port
        self.compression = compression

        if auth_provider is not None:
            if not callable(auth_provider):
                raise ValueError("auth_provider must be callable")
            self.auth_provider = auth_provider

        if load_balancing_policy is not None:
            self.load_balancing_policy = load_balancing_policy
        else:
            self.load_balancing_policy = RoundRobinPolicy()

        if reconnection_policy is not None:
            self.reconnection_policy = reconnection_policy

        if default_retry_policy is not None:
            self.default_retry_policy = default_retry_policy

        if conviction_policy_factory is not None:
            if not callable(conviction_policy_factory):
                raise ValueError("conviction_policy_factory must be callable")
            self.conviction_policy_factory = conviction_policy_factory

        if connection_class is not None:
            self.connection_class = connection_class

        self.metrics_enabled = metrics_enabled
        self.ssl_options = ssl_options
        self.sockopts = sockopts
        self.cql_version = cql_version
        self.max_schema_agreement_wait = max_schema_agreement_wait

        self._listeners = set()
        self._listener_lock = Lock()

        # let Session objects be GC'ed (and shutdown) when the user no longer
        # holds a reference. Normally the cycle detector would handle this,
        # but implementing __del__ prevents that.
        self.sessions = WeakSet()
        self.metadata = Metadata(self)
        self.control_connection = None
        self._prepared_statements = WeakValueDictionary()

        self._min_requests_per_connection = {
            HostDistance.LOCAL: cluster.DEFAULT_MIN_REQUESTS,
            HostDistance.REMOTE: cluster.DEFAULT_MIN_REQUESTS
        }

        self._max_requests_per_connection = {
            HostDistance.LOCAL: cluster.DEFAULT_MAX_REQUESTS,
            HostDistance.REMOTE: cluster.DEFAULT_MAX_REQUESTS
        }

        self._core_connections_per_host = {
            HostDistance.LOCAL: cluster.DEFAULT_MIN_CONNECTIONS_PER_LOCAL_HOST,
            HostDistance.REMOTE: cluster.DEFAULT_MIN_CONNECTIONS_PER_REMOTE_HOST
        }

        self._max_connections_per_host = {
            HostDistance.LOCAL: cluster.DEFAULT_MAX_CONNECTIONS_PER_LOCAL_HOST,
            HostDistance.REMOTE: cluster.DEFAULT_MAX_CONNECTIONS_PER_REMOTE_HOST
        }

        self.executor = ThreadPoolExecutor(max_workers=executor_threads)
        self.scheduler = cluster._Scheduler(self.executor)

        self._lock = RLock()

        if self.metrics_enabled:
            self.metrics = Metrics(weakref.proxy(self))

        self.control_connection = cluster.ControlConnection(self)
