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
from magnetodb.common.utils import statsd
from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class RequestMetricsMiddleware(wsgi.Middleware):
    """Middleware that enable collecting request metrics.

    Put this filter in the pipeline of api-paste.ini to turn on request metrics
    collecting.

    Note that only data api requests will participate in request metrics
    collection.
    """
    REQ_RESPONSE_SIZE_BYTES = "mdb.req.response_size_bytes"
    REQ_REQUEST_SIZE_BYTES = "mdb.req.request_size_bytes"

    REQ_TIMING = "mdb.req.timing"
    REQ_TIMING_ERROR = "mdb.req.timing.error"
    REQ_REQUEST_SIZE_BYTES_ERROR = "mdb.req.request_size_bytes.error"
    REQ_RESPONSE_SIZE_BYTES_ERROR = "mdb.req.response_size_bytes.error"

    METRICS_FIELD_SEPARATOR_CHAR = '.'

    def __init__(self, app, options):
        self.options = options
        self.statsd_client = statsd.StatsdClient.from_config()

        super(RequestMetricsMiddleware, self).__init__(app)

    @webob.dec.wsgify
    def __call__(self, req):
        start = time.time()
        response = req.get_response(self.application)

        tenant_id = get_tenant_id_data_api(req)
        if not tenant_id:
            # only data api will participate in request metrics collection
            return response

        content_length = req.content_length or 0

        if self.statsd_client and self.statsd_client.enabled:
            if is_success(response.status_int):
                timing_metrics_name = self.REQ_TIMING
                req_size_metrics_name = self.REQ_REQUEST_SIZE_BYTES
                response_size_metrics_name = self.REQ_RESPONSE_SIZE_BYTES
            else:
                timing_metrics_name = self.METRICS_FIELD_SEPARATOR_CHAR.join(
                    [self.REQ_TIMING_ERROR, str(response.status_int)]
                )
                req_size_metrics_name = self.METRICS_FIELD_SEPARATOR_CHAR.join(
                    [self.REQ_REQUEST_SIZE_BYTES_ERROR,
                     str(response.status_int)]
                )
                response_size_metrics_name = (
                    self.METRICS_FIELD_SEPARATOR_CHAR.join(
                        [self.REQ_RESPONSE_SIZE_BYTES_ERROR,
                         str(response.status_int)])
                )

            self.statsd_client.timing(timing_metrics_name, time.time() - start)
            self.statsd_client.increment(req_size_metrics_name,
                                         content_length)
            self.statsd_client.increment(response_size_metrics_name,
                                         response.content_length)

            if self.statsd_client.enabled_tenant:
                if is_success(response.status_int):
                    timing_metrics_name = (
                        self.METRICS_FIELD_SEPARATOR_CHAR.join(
                            [self.REQ_TIMING, tenant_id]))
                    req_size_metrics_name = (
                        self.METRICS_FIELD_SEPARATOR_CHAR.join(
                            [self.REQ_REQUEST_SIZE_BYTES, tenant_id])
                    )
                    response_size_metrics_name = (
                        self.METRICS_FIELD_SEPARATOR_CHAR.join(
                            [self.REQ_RESPONSE_SIZE_BYTES, tenant_id]
                        )
                    )
                else:
                    timing_metrics_name = (
                        self.METRICS_FIELD_SEPARATOR_CHAR.join(
                            [self.REQ_TIMING_ERROR,
                             str(response.status_int), tenant_id]
                        ))
                    req_size_metrics_name = (
                        self.METRICS_FIELD_SEPARATOR_CHAR.join(
                            [self.REQ_REQUEST_SIZE_BYTES_ERROR,
                             str(response.status_int), tenant_id]
                        ))
                    response_size_metrics_name = (
                        self.METRICS_FIELD_SEPARATOR_CHAR.join(
                            [self.REQ_RESPONSE_SIZE_BYTES_ERROR,
                             str(response.status_int), tenant_id]
                        )
                    )

                self.statsd_client.timing(timing_metrics_name,
                                          time.time() - start)
                self.statsd_client.increment(req_size_metrics_name,
                                             content_length)
                self.statsd_client.increment(response_size_metrics_name,
                                             response.content_length)

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
