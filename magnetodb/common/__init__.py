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

PROJECT_NAME = 'magnetodb'


def get_root_dir(start_search_path, root_dir_name):
    cur_path = os.path.abspath(start_search_path)
    while (os.path.basename(cur_path) != root_dir_name or
           os.path.exists(os.path.join(cur_path, '__init__.py'))):
        new_cur_path = os.path.dirname(cur_path)
        if (not new_cur_path) or (new_cur_path == cur_path):
            raise RuntimeError("Can't find project root directory.")
        cur_path = new_cur_path
    return cur_path

PROJECT_ROOT_DIR = get_root_dir(__file__, PROJECT_NAME)
