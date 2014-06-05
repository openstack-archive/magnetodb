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

from magnetodb.openstack.common.notifier import api as notifier_api
from magnetodb.openstack.common.notifier import test_notifier

from oslo.config import cfg


DATETIMEFORMAT = "%Y-%m-%d %H:%M:%S.%f"
CONF = cfg.CONF


class TestNotify(unittest.TestCase):
    """Unit tests for event notifier."""

    @classmethod
    def tearDownClass(cls):
        cls.cleanup_test_notifier()

    @classmethod
    def cleanup_test_notifier(cls):
        test_notifier.NOTIFICATIONS = []

    @classmethod
    def setup_notification_driver(cls, notification_driver=None):
        # clean up the notification queues and drivers
        cls.cleanup_test_notifier()
        notifier_api._reset_drivers()

        if notification_driver is None:
            notification_driver = [test_notifier.__name__]

        CONF.set_override("notification_driver", notification_driver)

    @classmethod
    def setUpClass(cls):
        cls.setup_notification_driver(notification_driver=None)
