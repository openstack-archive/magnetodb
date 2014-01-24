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

import re
from magnetodb.openstack.common import exception as openstack_exception
from magnetodb.openstack.common import log as logging

from webob.exc import HTTPException
from webob.response import Response


LOG = logging.getLogger(__name__)


def safe_fmt_string(text):
    return re.sub(r'%([0-9]+)', r'\1', text)


class MagnetoError(openstack_exception.OpenstackException):
    """Base exception that all custom MagnetoDB app exceptions inherit from."""
    internal_message = None

    def __init__(self, message=None, **kwargs):
        if message is not None:
            self.message = message
        if self.internal_message is not None:
            try:
                LOG.error(safe_fmt_string(self.internal_message) % kwargs)
            except Exception:
                LOG.error(self.internal_message)
        self.message = safe_fmt_string(self.message)
        super(MagnetoError, self).__init__(**kwargs)


class BackendInteractionException(MagnetoError):
    pass


class TableNotExistsException(BackendInteractionException):
    pass


class TableAlreadyExistsException(BackendInteractionException):
    pass


class AWSErrorResponseException(HTTPException, Response):
    """ Base Exception for rendering to AWS DynamoDB error
    JSON http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/
                                                            ErrorHandling.html
    """
    response_message = (
        "The server encountered an internal error"
        " trying to fulfill the request."
    )
    error_code = 'InternalServerError'
    status = '500'

    def __init__(self, message='Exception'):
        Response.__init__(self, status=self.status)
        Exception.__init__(self, message)

    def __call__(self, environ, start_response):
        response_headers = [('Content-type', 'application/x-amz-json-1.0')]
        start_response(self.status, response_headers)
        return (
            '{{"__type":"com.amazonaws.dynamodb.v20111205#{}","message":"{}"}}'
            .format(self.error_code, self.response_message)
        )


class BadRequestException(AWSErrorResponseException):
    """ Base class for all errors with HTTP status code 400"""
    status = '400'


class ResourceNotFoundException(BadRequestException):
    response_message = 'The resource which is being requested does not exist.'
    error_code = 'ResourceNotFoundException'


class ValidationException(BadRequestException):
    response_message = 'One or more required parameter values were missing.'
    error_code = 'ValidationException'


class ResourceInUseException(BadRequestException):
    response_message = (
        'The resource which you are attempting to change is in use.'
    )
    error_code = 'ResourceInUseException'
