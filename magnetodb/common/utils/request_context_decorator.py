# Copyright 2015 Symantec Corporation.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
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
import functools

from oslo_context import context as req_context


def context_update_store_wrapper(context, func, *args, **kwargs):
    context.update_store_by_weakref()
    return func(*args, **kwargs)


def request_type(event, **dec_kwargs):
    """
    Returns a decorator that sets value for request.context.request_type in
    MagnetoDB API endpoint controllers. The request_type value will be used by
    Notifier to look up the corresponding event in Event_Registry.
    """

    def decorating_func(func):

        @functools.wraps(func)
        def _request_type(*args, **kwargs):
            req_context.get_current().request_type = event
            resp = func(*args, **kwargs)
            return resp

        return _request_type

    return decorating_func
