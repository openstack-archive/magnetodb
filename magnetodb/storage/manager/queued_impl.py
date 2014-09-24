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
from oslo import messaging

from magnetodb.common import config
from magnetodb.storage.manager.base_async_storage_manager import (
    BaseAsyncStorageManager)


LOG = logging.getLogger(__name__)
CONF = config.CONF


class QueuedStorageManager(BaseAsyncStorageManager):
    def __init__(self, storage_driver,
                 table_info_repo,
                 concurrent_tasks=1000, batch_chunk_size=25):
        BaseAsyncStorageManager.__init__(
            self, storage_driver, table_info_repo,
            concurrent_tasks, batch_chunk_size)

        transport = messaging.get_transport(CONF)
        target = messaging.Target(topic='schema')

        self._rpc_client = messaging.RPCClient(transport, target)

    def _async_create(self, context, table_info):
        self._rpc_client.cast(
            context.to_dict(), 'create', table_name=table_info.name)

    def _async_delete(self, context, table_info):
        self._rpc_client.cast(
            context.to_dict(), 'delete', table_name=table_info.name)
