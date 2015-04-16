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

import unittest

from oslo_config import cfg
from oslo_messaging.notify import _impl_test

from magnetodb import context as req_context


DATETIMEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
CONF = cfg.CONF


class TestNotify(unittest.TestCase):
    """Unit tests for event notifier."""

    @classmethod
    def tearDownClass(cls):
        cls.cleanup_test_notifier()

    @classmethod
    def cleanup_test_notifier(cls):
        _impl_test.reset()

    @classmethod
    def setup_notification_driver(cls):
        # clean up the notification queues and drivers
        cls.cleanup_test_notifier()

    @classmethod
    def setUpClass(cls):
        cls.setup_notification_driver()

    def setUp(self):
        req_context.RequestContext()

    @classmethod
    def get_notifications(cls):
        return [message
                for (ctxt, message, priority, retry)
                in _impl_test.NOTIFICATIONS]
