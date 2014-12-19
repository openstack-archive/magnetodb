# Copyright 2015 Symantec Corporation
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import time
import webob

from magnetodb.common import wsgi
from magnetodb.common.middleware import rate_limit
from magnetodb import notifier
from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class RequestMetricsMiddleware(wsgi.Middleware):
    """Middleware that enable collecting request metrics.

    Put this filter in the pipeline of api-paste.ini to turn on request metrics
    collecting.

    Note that only data api requests will participate in request metrics
    collection.
    """
    ERROR = "error"
    METRICS_FIELD_SEPARATOR_CHAR = "."

    def __init__(self, app, options):
        self.options = options
        self._notifier = notifier.get_notifier()

        super(RequestMetricsMiddleware, self).__init__(app)

    @webob.dec.wsgify
    def __call__(self, req):
        start_time = time.time()

        tenant_id = get_tenant_id_data_api(req)
        response = req.get_response(self.application)

        if not tenant_id:
            # only data api requests will participate in notification/request
            # metrics collection
            return response

        context = req.context
        content_length = req.content_length or 0

        if is_success(response.status_int):
            msg = dict(value=start_time)
            request_type = ''
            if hasattr(context, 'request_type'):
                request_type = context.request_type
            if (hasattr(context, 'message') and
                    context.message and
                    isinstance(context.message, dict)):
                msg.update(context.message)
            self._notifier.notify(context, request_type, msg)

            self._notifier.notify(
                context,
                notifier.EVENT_TYPE_REQUEST_REQ_SIZE_BYTES,
                dict(value=content_length))

            self._notifier.notify(
                context,
                notifier.EVENT_TYPE_REQUEST_RSP_SIZE_BYTES,
                dict(value=response.content_length))

            self._notifier.notify(
                context,
                notifier.EVENT_TYPE_REQUEST_TIMING,
                dict(value=start_time))
        else:
            request_type = ''
            if hasattr(context, 'request_type'):
                request_type = (self.METRICS_FIELD_SEPARATOR_CHAR.join(
                    [context.request_type,
                     self.ERROR,
                     str(response.status_int)])
                )
            msg = dict(value=start_time)
            if (hasattr(context, 'message') and
                    context.message and
                    isinstance(context.message, dict)):
                msg.update(context.message)
            self._notifier.notify(context, request_type, msg)

            event_name = self.METRICS_FIELD_SEPARATOR_CHAR.join(
                [notifier.EVENT_TYPE_REQUEST_REQ_SIZE_BYTES_ERROR,
                 str(response.status_int)]
            )
            self._notifier.notify(
                context,
                event_name,
                dict(value=content_length))

            event_name = self.METRICS_FIELD_SEPARATOR_CHAR.join(
                [notifier.EVENT_TYPE_REQUEST_RSP_SIZE_BYTES_ERROR,
                 str(response.status_int)]
            )
            self._notifier.notify(
                context,
                event_name,
                dict(value=response.content_length))

            event_name = self.METRICS_FIELD_SEPARATOR_CHAR.join(
                [notifier.EVENT_TYPE_REQUEST_TIMING_ERROR,
                 str(response.status_int)]
            )
            self._notifier.notify(
                context,
                event_name,
                dict(value=start_time))

        self._notifier.clean_up_context(context)
        return response

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)


def is_success(status):
    """
    Check if HTTP status code is successful.

    :param status: http status code
    :returns: True if status is successful, else False
    """
    return 200 <= status <= 299


def get_tenant_id_data_api(req):
    """
    Get the project_id or tenant_id from request.

    :param req: http request
    :returns: project_id/tenant_id if request url matches data API url pattern,
    else None
    """
    return rate_limit.get_tenant_id(req.path)
