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

LOG = logging.getLogger(__name__)


class MagnetoException(openstack_exception.OpenstackException):
    """Base exception that all custom MagnetoDB app exceptions inherit from.
    MagnetoError should be handled on controller level
    """

    def __init__(self, message=None, **kwargs):
        if message is not None:
            self.message = message
        super(MagnetoException, self).__init__(**kwargs)


class BackendInteractionException(MagnetoException):
    """Base class for backend exceptions what should be
    rendered in response and handled by client
    """
    pass


class BackendInteractionError(MagnetoException):
    """Base exception class for indicating internal errors"""
    pass


class ValidationError(MagnetoException):
    pass


class Forbidden(MagnetoException):
    """Caller is not authorized for operation.
    HTTP Status Code: 403
    """
    pass


class RequestQuotaExceeded(MagnetoException):
    """Server is overloaded or caller has exceeded request quota.
    HTTP Status Code: 429
    """
    pass


class TableNotExistsException(BackendInteractionException):
    pass


class TableAlreadyExistsException(BackendInteractionException):
    pass


class ResourceInUseException(BackendInteractionException):
    pass


class InvalidQueryParameter(BackendInteractionException):
    pass


class ConditionalCheckFailedException(BackendInteractionException):
    def __init__(self, message='The conditional request failed', **kwargs):
        super(ConditionalCheckFailedException, self).__init__(
            message, **kwargs
        )


class ConfigNotFound(MagnetoException):
    def __init__(self, **kwargs):
        message = "Could not find config at %s" % kwargs.get('path')
        super(ConfigNotFound, self).__init__(message, **kwargs)


class BackupNotExists(BackendInteractionException):
    pass


class RestoreJobNotExists(BackendInteractionException):
    pass


class ContainerNotExists(MagnetoException):
    def __init__(self, **kwargs):
        message = "Container '%(container_name)s' not exists" % kwargs
        super(ContainerNotExists, self).__init__(message, **kwargs)


class ContainerDeletionError(MagnetoException):
    def __init__(self, **kwargs):
        message = ("Error while deleting container '%(container_name)s'" %
                   kwargs)
        super(ContainerDeletionError, self).__init__(message, **kwargs)


class DataDownloadError(MagnetoException):
    def __init__(self, **kwargs):
        message = ("Error while trying to retrieve data from container"
                   "'%(container_name)s' object '%(object_name)s'" % kwargs)
        super(DataDownloadError, self).__init__(message, **kwargs)


class DataUploadError(MagnetoException):
    def __init__(self, **kwargs):
        message = ("Error while trying to upload data to container"
                   "'%(container_name)s' object '%(object_name)s'" % kwargs)
        super(DataUploadError, self).__init__(message, **kwargs)
