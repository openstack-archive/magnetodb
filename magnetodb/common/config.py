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

from oslo_config import cfg

from magnetodb import common as mdb_common

common_opts = [
    cfg.StrOpt('storage_manager_config', default="{}"),
]

CONF = cfg.CONF
CONF.register_opts(common_opts)


def parse_args(prog=None, args=None, default_config_files=None):
    cfg.CONF(args=args,
             project=mdb_common.PROJECT_NAME,
             prog=prog,
             default_config_files=default_config_files)
