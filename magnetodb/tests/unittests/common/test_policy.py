# Copyright 2011 Piston Cloud Computing, Inc.
# All Rights Reserved.

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

"""Test of Policy Engine For Nova."""

import StringIO
import unittest
import mock
import testtools

from magnetodb.common.middleware import context
from magnetodb.common import exception
from magnetodb.openstack.common import policy as common_policy
from magnetodb import policy
from magnetodb.tests.unittests.common import policy_fixture


def build_context(roles='member'):
    options = {'auth_token': 'fake', 'tenant_id': 'fake', 'user_id': 'fake'}
    return context.ContextMiddleware('fake', options).make_context(roles=roles)


class PolicyFileTestCase(testtools.TestCase):
    def setUp(self):
        super(PolicyFileTestCase, self).setUp()
        self.context = build_context()
        self.target = {}

    def test_modified_policy_reloads(self):
        tmpfilename = '/tmp/policy.json'
        policy.cfg.CONF.find_file = mock.MagicMock(return_value=tmpfilename)
        action = "example:test"
        with open(tmpfilename, "w") as policyfile:
            policyfile.write('{"example:test": ""}')
        f = open(tmpfilename, 'r')
        print(f.readline())
        policy.enforce(self.context, action, self.target)
        with open(tmpfilename, "w") as policyfile:
            policyfile.write('{"example:test": "!"}')
        policy._POLICY_CACHE = {}
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, action, self.target)


class PolicyTestCase(testtools.TestCase):
    def setUp(self):
        super(PolicyTestCase, self).setUp()
        rules = {
            "true": '@',
            "example:allowed": '@',
            "example:denied": "!",
            "example:get_http": "http://www.example.com",
            "example:my_file": "role:compute_admin or "
                               "tenant:%(tenant_id)s",
            "example:early_and_fail": "! and @",
            "example:early_or_success": "@ or !",
            "example:lowercase_admin": "role:admin or role:sysadmin",
            "example:uppercase_admin": "role:ADMIN or role:sysadmin",
        }
        self.policy = self.useFixture(policy_fixture.PolicyFixture())
        self.policy.set_rules(rules)
        self.context = build_context()
        self.target = {}
        self.addCleanup(policy.reset)

    def test_enforce_nonexistent_action_throws(self):
        action = "example:noexist"
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, action, self.target)

    def test_enforce_bad_action_throws(self):
        action = "example:denied"
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, action, self.target)

    def test_enforce_bad_action_noraise(self):
        action = "example:denied"
        result = policy.enforce(self.context, action, self.target, False)
        self.assertEqual(result, False)

    def test_enforce_good_action(self):
        action = "example:allowed"
        result = policy.enforce(self.context, action, self.target)
        self.assertEqual(result, True)

    @mock.patch('urllib2.urlopen')
    def test_enforce_http_true(self, mock_urlopen):
        mock_urlopen.return_value = StringIO.StringIO("True")
        action = "example:get_http"
        target = {}
        result = policy.enforce(self.context, action, target)
        self.assertEqual(result, True)

    @mock.patch('urllib2.urlopen')
    def test_enforce_http_false(self, mock_urlopen):
        mock_urlopen.return_value = StringIO.StringIO("False")
        action = "example:get_http"
        target = {}
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, action, target)

    def test_templatized_enforcement(self):
        target_mine = {'tenant_id': 'fake'}
        target_not_mine = {'tenant_id': 'another'}
        action = "example:my_file"
        policy.enforce(self.context, action, target_mine)
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, action, target_not_mine)

    def test_early_AND_enforcement(self):
        action = "example:early_and_fail"
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, action, self.target)

    def test_early_OR_enforcement(self):
        action = "example:early_or_success"
        policy.enforce(self.context, action, self.target)

    def test_ignore_case_role_check(self):
        lowercase_action = "example:lowercase_admin"
        uppercase_action = "example:uppercase_admin"
        # NOTE(dprince) we mix case in the Admin role here to ensure
        # case is ignored
        admin_context = build_context(roles='AdmiN')
        policy.enforce(admin_context, lowercase_action, self.target)
        policy.enforce(admin_context, uppercase_action, self.target)


class DefaultPolicyTestCase(testtools.TestCase):

    def setUp(self):
        super(DefaultPolicyTestCase, self).setUp()
        self.policy = self.useFixture(policy_fixture.PolicyFixture())

        self.rules = {
            "default": '',
            "example:exist": "!",
        }

        self._set_rules('default')
        self.context = build_context()

    def _set_rules(self, default_rule):
        rules = common_policy.Rules(
            dict((k, common_policy.parse_rule(v))
                 for k, v in self.rules.items()), default_rule)
        common_policy.set_rules(rules)

    def test_policy_called(self):
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, "example:exist", {})

    def test_not_found_policy_calls_default(self):
        policy.enforce(self.context, "example:noexist", {})

    def test_default_not_found(self):
        self._set_rules("default_noexist")
        self.assertRaises(exception.Forbidden, policy.enforce,
                          self.context, "example:noexist", {})


class IsAdminCheckTestCase(unittest.TestCase):
    def test_init_true(self):
        check = policy.IsAdminCheck('is_admin', 'True')

        self.assertEqual(check.kind, 'is_admin')
        self.assertEqual(check.match, 'True')
        self.assertEqual(check.expected, True)

    def test_init_false(self):
        check = policy.IsAdminCheck('is_admin', 'nottrue')

        self.assertEqual(check.kind, 'is_admin')
        self.assertEqual(check.match, 'False')
        self.assertEqual(check.expected, False)

    def test_call_true(self):
        check = policy.IsAdminCheck('is_admin', 'True')

        self.assertEqual(check('target', dict(is_admin=True)), True)
        self.assertEqual(check('target', dict(is_admin=False)), False)

    def test_call_false(self):
        check = policy.IsAdminCheck('is_admin', 'False')

        self.assertEqual(check('target', dict(is_admin=True)), False)
        self.assertEqual(check('target', dict(is_admin=False)), True)


class AdminRolePolicyTestCase(testtools.TestCase):
    def setUp(self):
        super(AdminRolePolicyTestCase, self).setUp()
        self.policy = self.useFixture(policy_fixture.RoleBasedPolicyFixture())
        self.context = build_context()
        self.actions = policy.get_rules().keys()
        self.target = {}

    def test_enforce_admin_actions_with_nonadmin_context_throws(self):
        """Check if non-admin context passed to admin actions throws
           Policy not authorized exception
        """
        for action in self.actions:
            self.assertRaises(exception.Forbidden, policy.enforce,
                              self.context, action, self.target)
