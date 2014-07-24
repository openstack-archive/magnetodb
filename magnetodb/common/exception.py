# Copyright 2014 Symantec Corporation
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

from magnetodb.openstack.common import exception as openstack_exception
from magnetodb.openstack.common import log as logging

from webob.exc import HTTPException


LOG = logging.getLogger(__name__)


class MagnetoError(openstack_exception.OpenstackException):
    """Base exception that all custom MagnetoDB app exceptions inherit from."""

    def __init__(self, message=None, **kwargs):
        if message is not None:
            self.message = message
        super(MagnetoError, self).__init__(**kwargs)


class BackendInteractionException(MagnetoError):
    pass


class TableNotExistsException(BackendInteractionException):
    pass


class TableAlreadyExistsException(BackendInteractionException):
    pass


class ConditionalCheckFailedException(BackendInteractionException):
    def __init__(self, message='The conditional request failed', **kwargs):
        super(ConditionalCheckFailedException, self).__init__(
            message, **kwargs
        )


# Common Errors
class InternalFailure(MagnetoError):
    """Unknown error, exception or failure.
    HTTP Status Code: 500
    """
    pass


class RequestQuotaExceeded(MagnetoError):
    """Server is overloaded or caller has exceeded request quota.
    HTTP Status Code: 429
    """
    pass


class OverLimit(MagnetoError):
    """Caller is exceeded data storage quota.
    HTTP Status Code: 413
    """
    pass


class InvalidClientToken(MagnetoError):
    """The Keystone token does not exist or expired.
    HTTP Status Code: 401
    """
    pass


class Forbidden(MagnetoError):
    """Caller is not authorized for operation.
    HTTP Status Code: 403
    """
    pass


class InvalidParameterCombination(MagnetoError):
    """Parameters that must not be used together were used together.
    HTTP Status Code: 400
    """
    pass


class InvalidParameterValue(MagnetoError):
    """An invalid or out-of-range value was supplied for the input parameter.
    HTTP Status Code: 400
    """
    pass


class InvalidQueryParameter(MagnetoError):
    """SThe query string is malformed or does not adhere to standards.
    HTTP Status Code: 400
    """
    pass


class MalformedQueryString(MagnetoError):
    """The query string contains a syntax error.
    HTTP Status Code: 404
    """
    pass


class MissingParameter(MagnetoError):
    """A required parameter for the specified action is not supplied.
    HTTP Status Code: 400
    """
    pass


class ServiceUnavailable(MagnetoError):
    """Temporary failure of the server.
    HTTP Status Code: 503
    """
    pass


class ValidationError(MagnetoError):
    """Value validation failed.
    HTTP Status Code: 400
    """
    pass


# DynamoDB Errors
class AWSErrorResponseException(HTTPException):
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

    def __init__(self, response_message=None, error_code=None, status=None):
        if response_message is not None:
            self.response_message = response_message
        if error_code is not None:
            self.error_code = error_code
        if status is not None:
            self.status = status

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


class IncompleteSignatureError(BadRequestException):
    response_message = (
        'The request signature does not conform to AWS standards.'
    )
    error_code = 'IncompleteSignature'


class AccessDeniedError(AWSErrorResponseException):
    """ Base class for all errors with HTTP status code 403"""
    status = '403'
    response_message = 'User is not authorized to perform action'
    error_code = 'AccessDenied'


class InvalidClientTokenIdError(AccessDeniedError):
    response_message = 'The certificate or AWS Key ID provided does not exist'
    error_code = 'InvalidClientTokenId'


class SignatureError(AccessDeniedError):
    response_message = (
        'The request signature calculated does not match the provided one'
    )
    error_code = 'SignatureError'
