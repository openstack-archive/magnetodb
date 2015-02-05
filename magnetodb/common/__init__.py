# Copyright 2013 Mirantis Inc.
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

import sys


if 'eventlet' in sys.modules:
    import eventlet

    if eventlet.patcher.is_monkey_patched('thread'):
        from eventlet import semaphore
        orig_mod = sys.modules.get('threading')
        if orig_mod is None:
            orig_mod = __import__('threading')
        patched_attr = getattr(semaphore, 'BoundedSemaphore', None)
        if patched_attr is not None:
            setattr(orig_mod, 'BoundedSemaphore', patched_attr)

PROJECT_NAME = "magnetodb"

__setup_complete = False


def is_global_env_ready():
    return __setup_complete


def setup_global_env(program=None, args=None):
    if not is_global_env_ready():
        if sys.version_info[0] < 3:
            reload(sys)
            sys.setdefaultencoding('utf-8')

        from magnetodb import notifier
        from magnetodb import storage
        from magnetodb.common import config
        from magnetodb.openstack.common import log

        config.parse_args(
            prog=program,
            args=args
        )
        log.setup(PROJECT_NAME)
        notifier.setup()
        storage.setup()

        global __setup_complete
        __setup_complete = True
