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


class MagnetoError(openstack_exception.OpenstackException):
    """Base exception that all custom MagnetoDB app exceptions inherit from."""

    def __init__(self, message=None, **kwargs):
        if message is not None:
            self.message = message
        super(MagnetoError, self).__init__(**kwargs)


class BackendInteractionException(MagnetoError):
    pass


class ValidationError(MagnetoError):
    pass


class Forbidden(MagnetoError):
    """Caller is not authorized for operation.
    HTTP Status Code: 403
    """
    pass


class RequestQuotaExceeded(MagnetoError):
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


class ConfigNotFound(MagnetoError):
    def __init__(self, **kwargs):
        message = "Could not find config at %s" % kwargs.get('path')
        super(ConfigNotFound, self).__init__(message, **kwargs)


class BackupNotExists(BackendInteractionException):
    pass


class ContainerNotExists(MagnetoError):
    def __init__(self, **kwargs):
        message = "Container '%(container_name)s' not exists" % kwargs
        super(ConfigNotFound, self).__init__(message, **kwargs)


class ContainerDeletionError(MagnetoError):
    def __init__(self, **kwargs):
        message = "Error while deleting container '%(container_name)s'" % kwargs
        super(ContainerDeletionError, self).__init__(message, **kwargs)


class DataDownloadError(MagnetoError):
    def __init__(self, **kwargs):
        message = ("Error while trying to retrieve data from container"
                   "'%(container_name)s' object '%(object_name)s'" % kwargs
        super(DataDownloadError, self).__init__(message, **kwargs)


class DataUploadError(MagnetoError):
    def __init__(self, **kwargs):
        message = ("Error while trying to upload data to container"
                   "'%(container_name)s' object '%(object_name)s'" % kwargs
        super(DataUploadError, self).__init__(message, **kwargs)
