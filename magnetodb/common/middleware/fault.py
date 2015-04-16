# Copyright 2014 Mirantis Inc.
# Copyright 2014 Symantec Corporation
# Copyright 2013 Unitedstack Inc.
#
# Author: Jianing YANG (jianingy@unitedstack.com)
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""A middleware that turns exceptions into parsable string. Inspired by
Cinder's and Heat's faultwrappers
"""

import traceback

import webob

from magnetodb.common import exception
from oslo_middleware import base as wsgi

from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class FaultWrapper(wsgi.Middleware):
    """Replace error body with something the client can parse."""

    DEFAULT_WEBOB_EXCEPTION = webob.exc.HTTPInternalServerError()

    error_map = {
        # Common errors
        'RequestQuotaExceeded': webob.exc.HTTPTooManyRequests,
        'ServiceUnavailable': webob.exc.HTTPServiceUnavailable,
        'Forbidden': webob.exc.HTTPForbidden,
        'InvalidQueryParameter': webob.exc.HTTPBadRequest,
        'ValidationError': webob.exc.HTTPBadRequest,

        'ResourceInUseException': webob.exc.HTTPBadRequest,

        # Table errors
        'TableAlreadyExistsException': webob.exc.HTTPBadRequest,
        'TableNotExistsException': webob.exc.HTTPNotFound,

        # data item error
        'ConditionalCheckFailedException': webob.exc.HTTPBadRequest,
    }

    def __init__(self, app, options):
        self.options = options
        super(FaultWrapper, self).__init__(app)

    def _map_exception_to_error(self, class_exception):
        if class_exception == Exception:
            return webob.exc.HTTPInternalServerError

        if class_exception.__name__ not in self.error_map:
            return self._map_exception_to_error(class_exception.__base__)

        return self.error_map[class_exception.__name__]

    def _error(self, ex):
        webob_exc = self._map_exception_to_error(ex.__class__)

        ex_type = ex.__class__.__name__

        full_message = unicode(ex)
        if full_message.find('\n') > -1:
            message, msg_trace = full_message.split('\n', 1)
        else:
            msg_trace = traceback.format_exc()
            message = full_message

        trace = None
        if self.options.get('show_trace', False):
            trace = msg_trace

        error = {
            'code': webob_exc.code,
            'title': webob_exc.title,
            'explanation': webob_exc.explanation,
            'error': {
                'message': message,
                'type': ex_type,
                'traceback': trace,
            }
        }

        return error

    def _create_error_response(self, ex):
        error_info = self._error(ex)

        return webob.Response(
            json_body=error_info,
            status=error_info.get('code', self.DEFAULT_WEBOB_EXCEPTION.code)
        )

    def process_request(self, req):
        try:
            return req.get_response(self.application)
        except (exception.BackendInteractionException,
                exception.ValidationError) as ex:
            LOG.debug(ex)

            return self._create_error_response(ex)
        except Exception as ex:
            # some lower level exception. It is better to know about it
            # so, log the original message
            LOG.exception(ex)
            # but don't propagate internal details beyond here
            ex.args = (u'message="An Internal Error Occurred"',)
            return self._create_error_response(ex)

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
