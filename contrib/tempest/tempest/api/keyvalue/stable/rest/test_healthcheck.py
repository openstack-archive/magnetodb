# Copyright 2015 Mirantis Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the 'License'); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an 'AS IS' BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from tempest.api.keyvalue.rest_base.base import MagnetoDBTestCase


class MagnetoDBHealthcheckTest(MagnetoDBTestCase):

    def test_healthcheck(self):
        headers, body = self.client.healthcheck()
        self.assertEqual('200', headers['status'])
        self.assertEqual('{"API": "OK"}', body)

    def test_healthcheck_fullcheck(self):
        headers, body = self.client.healthcheck(fullcheck=True)
        self.assertEqual('200', headers['status'])
        results = body.split(',')
        self.assertEqual(4, len(results))
        for result in results:
            self.assertIn('OK', result)
