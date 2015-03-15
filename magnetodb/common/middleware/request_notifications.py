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
from magnetodb import notifier
from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class RequestNotificationsMiddleware(wsgi.Middleware):
    """Middleware that enable request based notifications.

    Put this filter in the pipeline of api-paste.ini to turn on request metrics
    collecting.

    Note that only data api requests will participate in request metrics
    collection.
    """

    def __init__(self, app, options):
        self.api_type = options["api_type"]
        self._notifier = notifier.get_notifier()

        super(RequestNotificationsMiddleware, self).__init__(app)

    @webob.dec.wsgify
    def __call__(self, req):
        start_time = time.time()

        response = req.get_response(self.application)

        request_type = "unknown"
        request_args = {}

        context = req.context if hasattr(req, 'context') else None
        if context is not None:
            if hasattr(context, 'request_type') and context.request_type:
                request_type = context.request_type
            if hasattr(context, 'request_args') and context.request_args:
                request_args = context.request_args

        event_type = notifier.create_request_event_type(
            self.api_type, request_type, response.status_code
        )

        payload = dict(value=start_time)

        if request_args:
            payload.update(request_args)

        if response.status_code >= 400:
            payload.update(error=response.body)

        payload.update(
            request_content_length=req.content_length or 0,
            response_content_length=response.content_length
        )
        self._notifier.notify(context, event_type, payload)
        return response

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
