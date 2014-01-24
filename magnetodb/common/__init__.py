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
PROJECT_NAME = "magnetodb"

__setup_complete = False


def is_global_env_ready():
    return __setup_complete


def setup_global_env(program=None, args=None):
    global __setup_complete
    assert not __setup_complete

    from magnetodb import storage
    from magnetodb.common import config
    from magnetodb.openstack.common import log
    from magnetodb.openstack.common import gettextutils

    gettextutils.install(PROJECT_NAME, lazy=False)

    config.parse_args(
        prog=program,
        args=args
    )
    log.setup(PROJECT_NAME)
    storage.setup()

    __setup_complete = True
