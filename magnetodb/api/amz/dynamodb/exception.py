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

from webob.exc import HTTPException


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


class AWSBadRequestException(AWSErrorResponseException):
    """ Base class for all errors with HTTP status code 400"""
    status = '400'


class AWSResourceNotFoundException(AWSBadRequestException):
    response_message = 'The resource which is being requested does not exist.'
    error_code = 'ResourceNotFoundException'


class AWSValidationException(AWSBadRequestException):
    response_message = 'One or more required parameter values were missing.'
    error_code = 'ValidationException'


class AWSResourceInUseException(AWSBadRequestException):
    response_message = (
        'The resource which you are attempting to change is in use.'
    )
    error_code = 'ResourceInUseException'


class AWSDuplicateTableError(AWSBadRequestException):
    def __init__(self, table_name):
        self.response_message = (
            "Table already exists: %s" % table_name
        )
    error_code = 'ResourceInUseException'


class AWSIncompleteSignatureError(AWSBadRequestException):
    response_message = (
        'The request signature does not conform to AWS standards.'
    )
    error_code = 'IncompleteSignature'


class AWSAccessDeniedError(AWSErrorResponseException):
    """ Base class for all errors with HTTP status code 403"""
    status = '403'
    response_message = 'User is not authorized to perform action'
    error_code = 'AccessDenied'


class AWSInvalidClientTokenIdError(AWSAccessDeniedError):
    response_message = 'The certificate or AWS Key ID provided does not exist'
    error_code = 'InvalidClientTokenId'


class AWSSignatureError(AWSAccessDeniedError):
    response_message = (
        'The request signature calculated does not match the provided one'
    )
    error_code = 'SignatureError'
