from itertools import chain
import logging

from greplin import scales

log = logging.getLogger(__name__)


class Metrics(object):

    request_timer = None
    """
    A :class:`greplin.scales.PmfStat` timer for requests. This is a dict-like
    object with the following keys:

      * count - number of requests that have been timed
      * min - min latency
      * max - max latency
      * mean - mean latency
      * stdev - standard deviation for latencies
      * median - median latency
      * 75percentile - 75th percentile latencies
      * 97percentile - 97th percentile latencies
      * 98percentile - 98th percentile latencies
      * 99percentile - 99th percentile latencies
      * 999percentile - 99.9th percentile latencies
    """

    connection_errors = None
    """
    A :class:`greplin.scales.IntStat` count of the number of times that a
    request to a Cassandra node has failed due to a connection problem.
    """

    write_timeouts = None
    """
    A :class:`greplin.scales.IntStat` count of write requests that resulted
    in a timeout.
    """

    read_timeouts = None
    """
    A :class:`greplin.scales.IntStat` count of read requests that resulted
    in a timeout.
    """

    unavailables = None
    """
    A :class:`greplin.scales.IntStat` count of write or read requests that
    failed due to an insufficient number of replicas being alive to meet
    the requested :class:`.ConsistencyLevel`.
    """

    other_errors = None
    """
    A :class:`greplin.scales.IntStat` count of all other request failures,
    including failures caused by invalid requests, bootstrapping nodes,
    overloaded nodes, etc.
    """

    retries = None
    """
    A :class:`greplin.scales.IntStat` count of the number of times a
    request was retried based on the :class:`.RetryPolicy` decision.
    """

    ignores = None
    """
    A :class:`greplin.scales.IntStat` count of the number of times a
    failed request was ignored based on the :class:`.RetryPolicy` decision.
    """

    known_hosts = None
    """
    A :class:`greplin.scales.IntStat` count of the number of nodes in
    the cluster that the driver is aware of, regardless of whether any
    connections are opened to those nodes.
    """

    connected_to = None
    """
    A :class:`greplin.scales.IntStat` count of the number of nodes that
    the driver currently has at least one connection open to.
    """

    open_connections = None
    """
    A :class:`greplin.scales.IntStat` count of the number connections
    the driver currently has open.
    """

    def __init__(self, cluster_proxy):
        log.debug("Starting metric capture")

        self.stats = scales.collection('/cassandra',
            scales.PmfStat('request_timer'),
            scales.IntStat('connection_errors'),
            scales.IntStat('write_timeouts'),
            scales.IntStat('read_timeouts'),
            scales.IntStat('unavailables'),
            scales.IntStat('other_errors'),
            scales.IntStat('retries'),
            scales.IntStat('ignores'),

            # gauges
            scales.Stat('known_hosts',
                lambda: len(cluster_proxy.metadata.all_hosts())),
            scales.Stat('connected_to',
                lambda: len(set(chain.from_iterable(s._pools.keys() for s in cluster_proxy.sessions)))),
            scales.Stat('open_connections',
                lambda: sum(sum(p.open_count for p in s._pools.values()) for s in cluster_proxy.sessions)))

        self.request_timer = self.stats.request_timer
        self.connection_errors = self.stats.connection_errors
        self.write_timeouts = self.stats.write_timeouts
        self.read_timeouts = self.stats.read_timeouts
        self.unavailables = self.stats.unavailables
        self.other_errors = self.stats.other_errors
        self.retries = self.stats.retries
        self.ignores = self.stats.ignores
        self.known_hosts = self.stats.known_hosts
        self.connected_to = self.stats.connected_to
        self.open_connections = self.stats.open_connections

    def on_connection_error(self):
        self.stats.connection_errors += 1

    def on_write_timeout(self):
        self.stats.write_timeouts += 1

    def on_read_timeout(self):
        self.stats.read_timeouts += 1

    def on_unavailable(self):
        self.stats.unavailables += 1

    def on_other_error(self):
        self.stats.other_errors += 1

    def on_ignore(self):
        self.stats.ignores += 1

    def on_retry(self):
        self.stats.retries += 1
