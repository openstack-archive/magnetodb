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

from oslo.config import cfg

from magnetodb.openstack.common.log import logging

LOG = logging.getLogger(__name__)

probe_opts = [
    cfg.BoolOpt('probe',
                default=True,
                help='Switches probe mechanism'),
    cfg.BoolOpt('suppress_args',
                default=True,
                help='Suppress args output'),
]

probe_group = cfg.OptGroup(name='probe',
                           title='Probe options')

CONF = cfg.CONF
CONF.register_group(probe_group)
CONF.register_opts(probe_opts, group='probe')


def checkpoint(name, id, elapsed, status, *args, **kwargs):
    msg = '{timer}, {id}, {checkpoint}, {elapsed} ms'.format(
        timer=name, id=id,
        checkpoint=status,
        elapsed=elapsed).strip()
    if not CONF.probe.suppress_args:
        msg += ' with args: {args}, {kwargs}'.format(args=args, kwargs=kwargs)
    LOG.info(msg)


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
        self.probe = CONF.probe.probe
        self.name = name
        if self.probe:
            self.thread_id = get_id(id)

    def __enter__(self):
        if self.probe:
            self.start = time.time()

    def __exit__(self, *args):
        if self.probe:
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
            with Probe(name, id):
                return f(*args, **kwargs)
        return wrapper
    return decorator
