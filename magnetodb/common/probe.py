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

import functools
import time
import threading

from oslo.config import cfg

from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)

probe_opts = [
    cfg.BoolOpt('enabled',
                default=False,
                help='Enables additional diagnostic log output'),
    cfg.BoolOpt('suppress_args',
                default=True,
                help='Suppresses args output'),
]

probe_group = cfg.OptGroup(name='probe',
                           title='Probe options')

CONF = cfg.CONF
CONF.register_group(probe_group)
CONF.register_opts(probe_opts, group='probe')


def checkpoint(name, id, elapsed, status, *args, **kwargs):
    msg = 'PROBE: {timer}, {id}, {checkpoint}, {elapsed} ms'.format(
        timer=name, id=id,
        checkpoint=status,
        elapsed=elapsed).strip()
    if not CONF.probe.suppress_args:
        msg += ' with args: {args}, {kwargs}'.format(args=args, kwargs=kwargs)
    LOG.info(msg)


class Probe(object):
    """Probe can be used to instrument code to get execution time
    in miliseconds. It should be used with context manager.

    Usage example:
        with Probe("query") as t:
            query(req, body, project_id, table_name)

    Or as decorator:

    Usage example:
        @Probe("query")
        def query(req, body, project_id, table_name):
            ...
    """

    def __init__(self, name=''):
        self.name = name
        self.func_name = ''
        self.thread_var = threading.local()

    def __enter__(self):
        self.enabled = CONF.probe.enabled
        if self.enabled:
            self.thread_var.thread_id = threading.current_thread()
            self.thread_var.start = time.time()

    def __exit__(self, *args):
        if self.enabled:
            name = self.name
            if self.func_name:
                name = '.'.join([name, self.func_name])
            # miliseconds
            ms = (time.time() - self.thread_var.start) * 1000
            checkpoint(name, self.thread_var.thread_id, ms, 'finished')

    def __call__(self, f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            self.func_name = f.__name__
            with self:
                return f(*args, **kwargs)
        return wrapper
