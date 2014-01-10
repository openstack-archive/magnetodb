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
from paste import deploy

import wsgi_intercept
from wsgi_intercept import http_client_intercept

from magnetodb.common import config

CONF = config.CONF


def run_fake_magnetodb_api(paste_conf_file, app_name=None):
    from magnetodb.openstack.common import pastedeploy

    http_client_intercept.install()
    app = pastedeploy.paste_deploy_app(paste_conf_file, app_name, {})
    wsgi_intercept.add_wsgi_intercept("localhost", 8080, lambda: app)


def stop_fake_magnetodb_api():
    wsgi_intercept.remove_wsgi_intercept("localhost", 8080)
    wsgi_intercept.http_client_intercept.uninstall()
