# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Unitedstack Inc.
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

from magnetodb.common import wsgi

from magnetodb.openstack.common import log as logging

logger = logging.getLogger(__name__)


class Fault(object):

    def __init__(self, error):
        self.error = error

    @webob.dec.wsgify(RequestClass=wsgi.Request)
    def __call__(self, req):
        serializer = wsgi.ResponseSerializer()
        resp = serializer.serialize(self.error, req.content_type)
        default_webob_exc = webob.exc.HTTPInternalServerError()
        resp.status_code = self.error.get('code', default_webob_exc.code)
        return resp


class FaultWrapper(wsgi.Middleware):
    """Replace error body with something the client can parse."""

    error_map = {
        # Common errors
        'InternalFailure': webob.exc.HTTPInternalServerError,
        'RequestQuotaExceeded': webob.exc.HTTPTooManyRequests,
        'OverLimit': webob.exc.HTTPRequestEntityTooLarge,
        'InvalidClientToken': webob.exc.HTTPUnauthorized,
        'Forbidden': webob.exc.HTTPForbidden,
        'MalformedQueryString': webob.exc.HTTPNotFound,
        'ServiceUnavailable': webob.exc.HTTPServiceUnavailable,
        'InvalidParameterCombination': webob.exc.HTTPBadRequest,
        'InvalidParameterValue': webob.exc.HTTPBadRequest,
        'InvalidQueryParameter': webob.exc.HTTPBadRequest,
        'ValidationError': webob.exc.HTTPBadRequest,
        'MissingParameter': webob.exc.HTTPBadRequest,

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

    def process_request(self, req):
        try:
            return req.get_response(self.application)
        except Exception as ex:
            logger.exception(ex)
            return req.get_response(Fault(self._error(ex)))

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
