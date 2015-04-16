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

import unittest
import mock
from magnetodb.common.middleware import fault
from magnetodb.common import exception


class TestFaultWrapper(unittest.TestCase):
    """Unit tests for FaultWrapper."""

    def setUp(self):
        self.request = mock.Mock()
        self.fault_wrapper = fault.FaultWrapper(None, dict())

    def _side_effect_generator(self, return_values):
        """
        Helper function to create the appropriate side_effect for Mock
        objects used in this file.

        In particular, this helper is for governing behavior when the
        same mocked method is invoked multiple times. We pop the first
        element in the list and take action based on that value.

        :param return_values: the list of values governing behavior at
                              each invocation
        :return: the side_effect which maybe passed to a Mock object
        """
        def side_effect(*args):
            result = return_values.pop(0)
            if isinstance(result, Exception):
                raise result
            if result == 'use args':
                return args
            return result
        return side_effect

    def test_process_request_no_exception(self):
        """Test the positive case response. """

        expected_response = "Request Succeeded"
        self.request.get_response = mock.Mock(return_value=expected_response)
        response = self.fault_wrapper.process_request(self.request)
        self.assertEqual(expected_response, response)

    def test_process_request_validation_error(self):
        """Test the case where a ValidationError exception occurs. """

        expected_message = "A validation error occurred"
        validation_ex = exception.ValidationError(expected_message)
        side_effect = self._side_effect_generator([validation_ex, "use args"])
        self.request.get_response = mock.Mock(side_effect=side_effect)
        response = self.fault_wrapper.process_request(self.request)
        message = response.json_body['error']['message']
        self.assertEqual(expected_message, message)

    def test_process_request_backend_exception(self):
        """Test the case where BackendInteractionException occurs. """

        expected_message = "A backend interaction error occurred"
        backend_ex = exception.BackendInteractionException(expected_message)
        side_effect = self._side_effect_generator([backend_ex, "use args"])
        self.request.get_response = mock.Mock(side_effect=side_effect)
        response = self.fault_wrapper.process_request(self.request)
        message = response.json_body['error']['message']
        self.assertEqual(expected_message, message)

    def test_process_request_low_level_exception(self):
        """Test case where a low level exception occurs. """

        ex_message = "Low level internal details message"
        ex = Exception(ex_message)
        side_effect = self._side_effect_generator([ex, "use args"])
        self.request.get_response = mock.Mock(side_effect=side_effect)
        response = self.fault_wrapper.process_request(self.request)
        message = response.json_body['error']['message']
        self.assertNotEqual(ex_message, message)
