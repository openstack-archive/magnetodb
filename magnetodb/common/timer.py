# Copyright 2014 Symantec Corporation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


import time
import eventlet

from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)


class Timer(object):
    """ Timer can be used to instrument code to get execution time
    in miliseconds. It should be used with context manager.

    Usage example:
        with Timer("query") as t:
            query(req, body, project_id, table_name)
    """

    def __init__(self, name='', id='', verbose=False):
        self.verbose = verbose
        self.name = name
        self.id = id if id else eventlet.greenthread.getcurrent()

    def __enter__(self):
        self.start = time.time()

    @property
    def elapsed(self):
        return (time.time() - self.start) * 1000  # milliseconds

    def __exit__(self, *args):
        self.ms = self.elapsed
        self.checkpoint('finished')

    def checkpoint(self, name=''):
        if self.verbose:
            msg = '{timer},{id},{checkpoint},{elapsed}'.format(
                timer=self.name, id=self.id,
                checkpoint=name,
                elapsed=self.elapsed, ).strip()
            print(msg)
            LOG.info(msg)
