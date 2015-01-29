# Copyright 2014 Mirantis Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import atexit

import logging
import time
import threading
import weakref

import cassandra
from cassandra import cluster as cassandra_cluster
from cassandra import protocol as cassandra_protocol
from magnetodb.common import exception
from cassandra import query as cassandra_query

LOG = logging.getLogger(__name__)

wait_for_schema_agreement_origin = (
    cassandra_cluster.ControlConnection.wait_for_schema_agreement
)


def wait_for_schema_agreement(control_con, connection=None, *args, **kwargs):
    for i in xrange(3):
        matched = wait_for_schema_agreement_origin(
            control_con, connection, *args, **kwargs
        )

        if matched:
            break

        # refresh schema version if schema agreement was stuck somehow

        query = cassandra_protocol.QueryMessage(
            "ALTER TABLE magnetodb.dummy WITH comment=''",
            consistency_level=cassandra.ConsistencyLevel.ONE
        )
        connection.wait_for_response(query)

    return matched

cassandra_cluster.ControlConnection.wait_for_schema_agreement = (
    wait_for_schema_agreement
)


def _monitor_control_connection(cluster_handler_ref):
    while True:
        cluster_handler = cluster_handler_ref()
        if cluster_handler is None or cluster_handler._is_closed():
            return

        try:
            if not cluster_handler._is_connected():
                cluster_handler._connect()
        except Exception:
            LOG.exception("Error during connecting to the cluster")

        # clean hardlink to give ability to remove object
        cluster_handler = None
        time.sleep(1)


class ClusterHandler(object):
    def __init__(self, cluster_params, query_timeout=2,
                 concurrent_queries=100):
        self.__closed = False
        self.__task_semaphore = threading.BoundedSemaphore(concurrent_queries)

        self.__cluster_params = cluster_params
        self.__query_timeout = query_timeout
        self.__connection_lock = threading.RLock()
        self.__cluster = None
        self.__session = None
        self.__connection_monitor_thread = threading.Thread(
            target=_monitor_control_connection, args=(weakref.ref(self),)
        )
        self.__connection_monitor_thread.start()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        self.__closed = True
        self.__connection_monitor_thread.join()
        if self.__cluster:
            self.__cluster.shutdown()

    def _is_connected(self):
        return (
            self.__cluster is not None and
            self.__cluster.control_connection is not None and
            self.__cluster.control_connection._connection is not None and
            not self.__cluster.control_connection._connection.is_closed
        )

    def _is_closed(self):
        return self.__closed

    def _connect(self):
        with self.__connection_lock:
            if self.__cluster is not None:
                self._disconnect()

            count = len(atexit._exithandlers)
            try:
                cluster = cassandra_cluster.Cluster(**self.__cluster_params)
                session = cluster.connect()
            finally:
                while len(atexit._exithandlers) > count:
                    atexit._exithandlers.pop()

            session.row_factory = cassandra_query.dict_factory
            session.default_timeout = self.__query_timeout
            self.__cluster = cluster
            self.__session = session

    def _disconnect(self):
        with self.__connection_lock:
            if self.__cluster is None:
                return
            self.__cluster.shutdown()
            self.__cluster = None
            self.__session = None

    def execute_query(self, query, consistent=False):
        if self.__cluster is None:
            raise ClusterIsNotConnectedException()
        ex = None
        if consistent:
            query = cassandra_cluster.SimpleStatement(
                query,
                consistency_level=cassandra_cluster.ConsistencyLevel.QUORUM
            )
        LOG.debug("Executing query {}".format(query))
        for x in range(3):
            try:
                with self.__task_semaphore:
                    return self.__session.execute(query)
            except cassandra_cluster.NoHostAvailable as e:
                LOG.warning("It seems connection was lost. Retrying...",
                            exc_info=1)
                ex = e
            except Exception as e:
                ex = e
                break
        if ex:
            msg = "Error executing query {}:{}".format(query, e.message)
            LOG.exception(msg)
            raise ex

    def check_table_status(self, keyspace_name, table_name, expected_exists,
                           indexed_field_list=()):
        LOG.debug("Checking table status ...")

        keyspace_meta = self.__cluster.metadata.keyspaces.get(
            keyspace_name
        )

        if keyspace_meta is None:
            raise exception.BackendInteractionError(
                "Keyspace '{}' does not exist".format(keyspace_name)
            )

        table_meta = keyspace_meta.tables.get(table_name)
        if expected_exists:
            if table_meta is None:
                raise SchemaUpdateException()
            for indexed_field in indexed_field_list:
                column = table_meta.columns.get(indexed_field)
                if not column.index:
                    raise SchemaUpdateException()
        else:
            if table_meta is not None:
                raise SchemaUpdateException()

        return True


class ClusterIsNotConnectedException(Exception):
    pass


class SchemaUpdateException(Exception):
    pass
