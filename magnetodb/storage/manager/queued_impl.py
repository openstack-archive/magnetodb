# Copyright 2014 Symantec Corporation
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

import oslo_messaging
from oslo_context import context as req_context

from magnetodb.common import config
from magnetodb.storage.manager import simple_impl as manager

LOG = logging.getLogger(__name__)
CONF = config.CONF


class QueuedStorageManager(manager.SimpleStorageManager):
    def __init__(self, storage_driver, table_info_repo,
                 concurrent_tasks=1000, batch_chunk_size=25,
                 schema_operation_timeout=300):
        manager.SimpleStorageManager.__init__(
            self, storage_driver, table_info_repo,
            concurrent_tasks, batch_chunk_size,
            schema_operation_timeout
        )

        transport = oslo_messaging.get_transport(CONF)
        target = oslo_messaging.Target(topic='schema')

        self._rpc_client = oslo_messaging.RPCClient(transport, target)

    def _do_create_table(self, tenant, table_info):
        self._rpc_client.cast(
            req_context.get_current().to_dict(), 'create',
            tenant=tenant, table_name=table_info.name
        )

    def _do_delete_table(self, tenant, table_info):
        self._rpc_client.cast(
            req_context.get_current().to_dict(), 'delete',
            tenant=tenant, table_name=table_info.name
        )
