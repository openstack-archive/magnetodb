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
import sys

from magnetodb.common import wsgi


class Debug(wsgi.Middleware):
    """
    Helper class that can be inserted into any WSGI application chain
    to get information about the request and response.
    """

    def process_request(self, req):
        print(("*" * 40) + " REQUEST ENVIRON")
        for key, value in req.environ.items():
            print(key, "=", value)
        print()

    def process_response(self, resp):
        print(("*" * 40) + " RESPONSE HEADERS")
        for (key, value) in resp.headers.iteritems():
            print(key, "=", value)
        print()

        return resp

    @staticmethod
    def print_generator(app_iter):
        """
        Iterator that prints the contents of a wrapper string iterator
        when iterated.
        """
        print(("*" * 40) + " BODY")
        for part in app_iter:
            sys.stdout.write(part)
            sys.stdout.flush()
            yield part
        print()


def factory_method(global_config, **local_config):
    return lambda application: Debug(application)