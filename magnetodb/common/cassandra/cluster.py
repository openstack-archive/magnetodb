from cassandra.cluster import Cluster as OriginalCluster
from cassandra.cluster import ControlConnection as OriginalControlConnection
from cassandra.cluster import log as original_log

from threading import RLock

from cassandra import (ConsistencyLevel, OperationTimedOut)
from cassandra.decoder import QueryMessage

from cassandra.query import dict_factory

log = original_log


class Cluster(OriginalCluster):
    def __init__(self, *args, **kwargs):
        self._schema_change_listeners = kwargs.pop(
            "schema_change_listeners", tuple()
        )
        super(Cluster, self).__init__(*args, **kwargs)
        self.control_connection = ControlConnection(
            self, self.control_connection_timeout)


class ControlConnection(OriginalControlConnection):

    def __init__(self, *args, **kwargs):
        super(ControlConnection, self).__init__(*args, **kwargs)
        self._schema_agreement_lock = RLock()

    def _handle_schema_change(self, event):
        keyspace = event['keyspace'] or None
        table = event['table'] or None
        if event['change_type'] in ("CREATED", "DROPPED"):
            keyspace = keyspace if table else None
            self._submit(self.refresh_schema, keyspace)
        elif event['change_type'] == "UPDATED":
            self._submit(self.refresh_schema, keyspace, table)

        for listener in self._cluster._schema_change_listeners:
            listener(event)

    def _refresh_schema(self, connection, keyspace=None, table=None):
        if self._cluster.is_shutdown:
            return

        with self._schema_agreement_lock:
            log.debug("start refresh_schema for cluster:{}".format(self._cluster))
            current_schema_version = self.wait_for_schema_agreement(connection)
            expected_schema_version = None
            while current_schema_version != expected_schema_version:
                expected_schema_version = current_schema_version

                where_clause = ""
                if keyspace:
                    where_clause = " WHERE keyspace_name = '%s'" % (keyspace,)
                    if table:
                        where_clause += " AND columnfamily_name = '%s'" % (table,)

                cl = ConsistencyLevel.ONE
                if table:
                    ks_query = None
                else:
                    ks_query = QueryMessage(query=self._SELECT_KEYSPACES + where_clause, consistency_level=cl)
                cf_query = QueryMessage(query=self._SELECT_COLUMN_FAMILIES + where_clause, consistency_level=cl)
                col_query = QueryMessage(query=self._SELECT_COLUMNS + where_clause, consistency_level=cl)

                if ks_query:
                    ks_result, cf_result, col_result = connection.wait_for_responses(
                        ks_query, cf_query, col_query, timeout=self._timeout)
                    ks_result = dict_factory(*ks_result.results)
                    cf_result = dict_factory(*cf_result.results)
                    col_result = dict_factory(*col_result.results)
                else:
                    ks_result = None
                    cf_result, col_result = connection.wait_for_responses(
                        cf_query, col_query, timeout=self._timeout)
                    cf_result = dict_factory(*cf_result.results)
                    col_result = dict_factory(*col_result.results)

                current_schema_version = self.wait_for_schema_agreement(connection)


            log.debug("[control connection] Fetched schema, rebuilding metadata")
            if table:
                self._cluster.metadata.table_changed(keyspace, table, cf_result, col_result)
            elif keyspace:
                self._cluster.metadata.keyspace_changed(keyspace, ks_result, cf_result, col_result)
            else:
                self._cluster.metadata.rebuild_schema(ks_result, cf_result, col_result)

    def wait_for_schema_agreement(self, connection=None):
        # Each schema change typically generates two schema refreshes, one
        # from the response type and one from the pushed notification. Holding
        # a lock is just a simple way to cut down on the number of schema queries
        # we'll make.
        with self._schema_agreement_lock:
            if self._is_shutdown:
                return

            log.debug("[control connection] Waiting for schema agreement")
            if not connection:
                connection = self._connection

            start = self._time.time()
            elapsed = 0
            cl = ConsistencyLevel.ONE
            total_timeout = self._cluster.max_schema_agreement_wait
            while elapsed < total_timeout:
                peers_query = QueryMessage(query=self._SELECT_SCHEMA_PEERS, consistency_level=cl)
                local_query = QueryMessage(query=self._SELECT_SCHEMA_LOCAL, consistency_level=cl)
                try:
                    timeout = min(2.0, total_timeout - elapsed)
                    peers_result, local_result = connection.wait_for_responses(
                        peers_query, local_query, timeout=timeout)
                except OperationTimedOut as timeout:
                    log.debug("[control connection] Timed out waiting for " \
                              "response during schema agreement check: %s", timeout)
                    elapsed = self._time.time() - start
                    continue

                peers_result = dict_factory(*peers_result.results)

                versions = set()
                if local_result.results:
                    local_row = dict_factory(*local_result.results)[0]
                    if local_row.get("schema_version"):
                        versions.add(local_row.get("schema_version"))

                for row in peers_result:
                    if not row.get("rpc_address") or not row.get("schema_version"):
                        continue

                    rpc = row.get("rpc_address")
                    if rpc == "0.0.0.0":  # TODO ipv6 check
                        rpc = row.get("peer")

                    peer = self._cluster.metadata.get_host(rpc)
                    if peer and peer.is_up:
                        versions.add(row.get("schema_version"))

                if len(versions) == 1:
                    version = iter(versions).next()
                    log.debug("[control connection] Schemas match, version: %s", version)
                    return version

                log.debug("[control connection] Schemas mismatched, trying again")
                self._time.sleep(0.2)
                elapsed = self._time.time() - start

            raise OperationTimedOut("Schema agreement timeout exceeded")
