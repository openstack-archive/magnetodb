# Copyright 2014 Symantec Corporation.
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

from oslo.config import cfg

from magnetodb.common import PROJECT_NAME

extra_notifier_opts = [
    cfg.StrOpt('notification_service',
               default=PROJECT_NAME,
               help='Service publisher_id for outgoing notifications')
]

cfg.CONF.register_opts(extra_notifier_opts)


def setup():
    pass
