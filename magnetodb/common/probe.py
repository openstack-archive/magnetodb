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


from eventlet import greenthread
import functools
import time

from magnetodb import notifier
from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)
verbose = True
supress_args = True


def checkpoint(name, id, elapsed, status, *args, **kwargs):
    if verbose:
        msg = '{timer}, {id}, {checkpoint}, {elapsed} ms'.format(
            timer=name, id=id,
            checkpoint=status,
            elapsed=elapsed).strip()
        if not supress_args:
            msg += ' with args: {args}, {kwargs}'.format(args=args,
                                                         kwargs=kwargs)
        LOG.info(msg)


def checkpoint_notify(context, name, id, elapsed, status, *args, **kwargs):
    if verbose:
        msg = '{timer}, {id}, {checkpoint}, {elapsed} ms'.format(
            timer=name, id=id,
            checkpoint=status,
            elapsed=elapsed).strip()
        if not supress_args:
            msg += ' with args: {args}, {kwargs}'.format(args=args,
                                                         kwargs=kwargs)
        notifier.notify(context, notifier.EVENT_TYPE_PROBE_FINISHED, msg)


def elapsed(start):
    return (time.time() - start) * 1000  # milliseconds


def get_id(thread_id):
    return thread_id if thread_id else greenthread.getcurrent()


class Probe(object):
    """ Probe can be used to instrument code to get execution time
    in miliseconds. It should be used with context manager.

    Usage example:
        with Probe("query") as t:
            query(req, body, project_id, table_name)
    """

    def __init__(self, name='', id=''):
        self.name = name
        self.thread_id = get_id(id)

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, *args):
        self.ms = elapsed(self.start)
        checkpoint(self.name, self.thread_id, self.ms, 'finished')


def probe(name='', id=''):
    """ Decorator can be used to instrument code to get function
    execution time in miliseconds.

    Usage example:
        @probe("query")
        def query(req, body, project_id, table_name):
            ...
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            start = time.time()
            thread_id = get_id(id)
            result = f(*args, **kwargs)
            ms = elapsed(start)
            checkpoint(name, thread_id, ms, 'finished',
                       *args, **kwargs)
            return result
        return wrapper
    return decorator


def probe_notify(name='', id=''):
    """ Decorator can be used to instrument code to get method
    execution time in miliseconds in thread and send a notification.

    Usage example:
        @probe_notify("create_table")
        def create_table(self, context, *args, **kwargs):
            ...
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(self, context, *args, **kwargs):
            start = time.time()
            thread_id = get_id(id)
            result = f(self, context, *args, **kwargs)
            ms = elapsed(start)
            checkpoint_notify(context, name, thread_id, ms, 'finished',
                              *args, **kwargs)
            return result
        return wrapper
    return decorator
