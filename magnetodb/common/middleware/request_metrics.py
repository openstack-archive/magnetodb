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

import re
import time
import urlparse
import webob

from magnetodb.common import wsgi
from magnetodb.common.utils import request_context_decorator
from magnetodb import notifier
from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class Url_Mapper:
    def __init__(self, url_pattern, method, request_type):
        self.url_regex = re.compile(url_pattern)
        self.method = method
        self.request_type = request_type


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
        self._mapper = [
            Url_Mapper("(?:^\/(?:[^\/]+?)\/batch_write_item)",
                       "POST",
                       "magnetodb.req.mdb.BatchWrite"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?)\/put_item)",
                       "POST",
                       "magnetodb.req.mdb.PutItem"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?)\/get_item)",
                       "POST",
                       "magnetodb.req.mdb.GetItem"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?)\/delete_item)",
                       "POST",
                       "magnetodb.req.mdb.DeleteItem"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?)\/update_item)",
                       "POST",
                       "magnetodb.req.mdb.UpdateItem"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/batch_get_item)",
                       "POST",
                       "magnetodb.req.mdb.BatchRead"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables)",
                       "GET",
                       "magnetodb.req.mdb.ListTables"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables)",
                       "POST",
                       "magnetodb.req.mdb.CreateTable"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?))",
                       "GET",
                       "magnetodb.req.mdb.DescribeTable"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?)\/scan)",
                       "POST",
                       "magnetodb.req.mdb.Scan"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?)\/query)",
                       "POST",
                       "magnetodb.req.mdb.Query"),
            Url_Mapper("(?:^\/(?:[^\/]+?)\/tables\/(?:[^\/]+?))",
                       "DELETE",
                       "magnetodb.req.mdb.DeleteTable")
        ]

        super(RequestMetricsMiddleware, self).__init__(app)

    @webob.dec.wsgify
    def __call__(self, req):
        start_time = time.time()

        response = req.get_response(self.application)
        context = req.context if hasattr(req, 'context') else None

        if context is not None:
            if (not context.tenant or
                    not hasattr(context, 'request_type') or
                    not context.request_type):
                # only data api requests with request_type defined in
                # request.context will participate in notification/request
                # metrics collection
                return response

        content_length = req.content_length or 0

        msg = dict(value=start_time)
        request_type = ''
        if context and hasattr(context, 'request_type'):
            request_type = context.request_type
        else:
            for url_mapper in self._mapper:
                url_path = urlparse.urlparse(req.url).path.rstrip('/')
                if url_path.startswith('/v1/data'):
                    url_path = url_path[len('/v1/data'):]
                if (url_mapper.url_regex.match(url_path) and
                        req.method == url_mapper.method):
                    request_type = url_mapper.request_type
                    break

        if not is_success(response.status_int):
            request_type = (self.METRICS_FIELD_SEPARATOR_CHAR.join(
                [request_type,
                 self.ERROR,
                 str(response.status_int)])
            )

        if (hasattr(context, 'message') and
                context.message and
                isinstance(context.message, dict)):
            msg.update(context.message)
        msg.update(
            dict(request_content_length=content_length,
                 response_content_length=response.content_length)
        )
        self._notifier.notify(context, request_type, msg)

        request_context_decorator.clean_up_context(context)
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
