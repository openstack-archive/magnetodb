# Copyright 2011 OpenStack Foundation
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
import wsgi_intercept
from wsgi_intercept import http_client_intercept

from magnetodb.common import config

from magnetodb.openstack.common import log

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

CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR, 'etc/magnetodb-test.conf')
PASTE_CONFIG_FILE = os.path.join(PROJECT_ROOT_DIR, 'etc/api-paste.ini')

CONF = config.CONF
config.parse_args(argv=[],
                  default_config_files=[CONFIG_FILE])

log.setup(PROJECT_NAME)


def run_fake_magnetodb_api():
    from magnetodb.openstack.common import pastedeploy

    http_client_intercept.install()

    app = pastedeploy.paste_deploy_app(PASTE_CONFIG_FILE, PROJECT_NAME, {})
    wsgi_intercept.add_wsgi_intercept(CONF.bind_host,
                                      CONF.bind_port,
                                      lambda: app)


def stop_fake_magnetodb_api():
    wsgi_intercept.remove_wsgi_intercept(CONF.bind_host,
                                         CONF.bind_port)
    wsgi_intercept.http_client_intercept.uninstall()
