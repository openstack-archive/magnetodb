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
from functools import wraps

from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)
verbose = True

def checkpoint(name, id, elapsed, status=''):
    if verbose:
        msg = '{timer},{id},{checkpoint},{elapsed}ms'.format(
            timer=name, id=id,
            checkpoint=status,
            elapsed=elapsed).strip()
        LOG.info(msg)


def elapsed(start):
    return (time.time() - start) * 1000  # milliseconds


class Timer(object):
    """ Timer can be used to instrument code to get execution time
    in miliseconds. It should be used with context manager.

    Usage example:
        with Timer("query") as t:
            query(req, body, project_id, table_name)
    """

    def __init__(self, name='', thread_id=''):
        self.name = name
        self.id = thread_id if thread_id else eventlet.greenthread.getcurrent()

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        self.ms = elapsed(self.start)
        checkpoint(self.name, self.id, self.ms, 'finished')


def timer(name='', thread_id=''):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = time.time()
            id = thread_id if thread_id else eventlet.greenthread.getcurrent()
            result = f(*args, **kwargs)
            ms = elapsed(start)
            checkpoint(name, id, ms, 'finished')
            return result
        return wrapper
    return decorator
