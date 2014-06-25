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

import logging

from threading import BoundedSemaphore
from threading import Lock
from threading import Thread

import time

from cassandra import cluster as cassandra_cluster
from magnetodb.common import exception
from cassandra import query as cassandra_query

LOG = logging.getLogger(__name__)


class ClusterHandler(object):
    def __init__(self, cluster_params, query_timeout=2,
                 concurrent_queries=100):
        self.__task_semaphore = BoundedSemaphore(concurrent_queries)

        self.__cluster_params = cluster_params
        self.__query_timeout = query_timeout
        self.__connection_lock = Lock()
        self.__cluster = None
        self.__session = None
        self.__connection_monitor_thread = Thread(
            target=self.__monitor_control_connection
        )
        self.__connection_monitor_thread.start()

    def __connect(self):
        with self.__connection_lock:
            assert self.__cluster is None
            cluster = cassandra_cluster.Cluster(**self.__cluster_params)
            session = cluster.connect()
            session.row_factory = cassandra_query.dict_factory
            session.default_timeout = self.__query_timeout
            self.__cluster = cluster
            self.__session = session

    def __disconnect(self):
        with self.__connection_lock:
            assert self.__cluster is not None
            self.__cluster.shutdown()
            self.__cluster = None
            self.__session = None

    def __monitor_control_connection(self):
        while True:
            try:
                if not self.__cluster:
                    self.__connect()
                elif self.__cluster.control_connection._connection.is_closed:
                    self.__disconnect()
                    self.__connect()
            except:
                LOG.exception("Error during connecting to the cluster")
            time.sleep(1)

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

    def wait_for_table_status(self, keyspace_name, table_name,
                              expected_exists):
        LOG.debug("Start waiting for table status changing...")

        while True:
            keyspace_meta = self.__cluster.metadata.keyspaces.get(
                keyspace_name
            )

            if keyspace_meta is None:
                raise exception.BackendInteractionException(
                    "Keyspace '{}' does not exist".format(keyspace_name)
                )

            table_meta = keyspace_meta.tables.get(table_name)
            if expected_exists == (table_meta is not None):
                self.__cluster.control_connection.wait_for_schema_agreement()
                break

            LOG.debug("Table status isn't correct "
                      "(expected_exists: %(expected_exists)s, "
                      "table_meta: %(table_meta)s). "
                      "Wait and check again.",
                      {'expected_exists': expected_exists,
                       'table_meta': table_meta})
            time.sleep(1)

        LOG.debug("Table status is correct "
                  "(expected_exists: %(expected_exists)s, "
                  "table_meta: %(table_meta)s).",
                  {'expected_exists': expected_exists,
                   'table_meta': table_meta})


class ClusterIsNotConnectedException(Exception):
    pass
