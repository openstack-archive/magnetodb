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

import os

from magnetodb.common import PROJECT_NAME
from magnetodb.common import setup_global_env
from magnetodb.common import reset_global_env


def get_root_source(start_search_path, root_dir_name):
    cur_path = os.path.abspath(start_search_path)
    while (os.path.basename(cur_path) != root_dir_name):
        new_cur_path = os.path.dirname(cur_path)
        if (not new_cur_path) or (new_cur_path == cur_path):
            raise RuntimeError("Can't find project root directory.")
        cur_path = new_cur_path
    return os.path.dirname(cur_path)

PROJECT_ROOT_DIR = get_root_source(__file__, PROJECT_NAME)


def setup_global_env_for_test(file_name="etc/magnetodb-api-test.conf"):
    if not file_name.startswith("/"):
        file_name = PROJECT_ROOT_DIR + "/" + file_name
    setup_global_env(program="magnetodb-api", args=[
        "--config-file", file_name
    ])


def reset_global_env_for_test():
    reset_global_env()

