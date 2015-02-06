# Copyright 2015 Reliance Jio Infocomm Ltd.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import httplib
import mock

from oslo_serialization import jsonutils as json

from magnetodb.common import exception
from magnetodb.tests.unittests.api.openstack.v1 import test_base_testcase


class ExceptionResponsesTest(test_base_testcase.APITestCase):
    """Test for messages returned when exceptions occur in API calls.

    This test uses the list_tables api call, since it is one of the
    simpler ones.
    """

    def _get_api_call_error(self):
        """Make the API call and return the response object."""

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json'}

        conn = httplib.HTTPConnection('localhost:8080')
        url = '/v1/data/default_tenant/tables'
        conn.request("GET", url, headers=headers)

        response = conn.getresponse()

        json_response = response.read()
        response_model = json.loads(json_response)
        return (response_model['code'],
                response_model['error']['message'])

    @mock.patch('magnetodb.storage.list_tables')
    def test_validation_error_message(self, mock_list_tables):
        """Test the error message received when a ValidationError occurs. """

        expected_message = 'There was some validation error'
        mock_list_tables.side_effect = \
            exception.ValidationError(expected_message)
        (code, message) = self._get_api_call_error()

        self.assertEqual(expected_message, message)
        self.assertEqual(400, code)

    @mock.patch('magnetodb.storage.list_tables')
    def test_backend_exception_message(self, mock_list_tables):
        """Test the error message received when a BackendInteractionException
           occurs.
        """

        expected_message = 'There was some backend interaction exception'
        mock_list_tables.side_effect = \
            exception.BackendInteractionException(expected_message)
        (code, message) = self._get_api_call_error()

        self.assertEqual(expected_message, message)
        self.assertEqual(500, code)

    @mock.patch('magnetodb.storage.list_tables')
    def test_low_level_exception_message(self, mock_list_tables):
        """Test the error message received when a low level exception
           occurs. In particular, the message should not expose low
           level details.
        """

        exception_message = 'This message contains low level details'
        mock_list_tables.side_effect = Exception(exception_message)
        (code, message) = self._get_api_call_error()

        self.assertNotEqual(exception_message, message)
        self.assertEqual(500, code)
