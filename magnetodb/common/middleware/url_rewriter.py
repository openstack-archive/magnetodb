# Copyright 2014 Symantec Corporation
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

from oslo_config import cfg

from oslo_middleware import base as wsgi
from magnetodb.openstack.common import log as logging

LOG = logging.getLogger(__name__)

url_rewriter_middleware_opts = [
    cfg.StrOpt('url_rewrite_protocol_override',
               default='',
               help="Use this value to override the original incoming "
                    "request protocol scheme. This has higher priority than "
                    "url_rewrite_protocol_header."),
    cfg.StrOpt('url_rewrite_protocol_header',
               default='X-Forwarded-Proto',
               help="The HTTP Header that will be used to determine which "
                    "the original request protocol scheme was, even if it"
                    "was removed by an SSL terminator proxy."),
    cfg.StrOpt('url_rewrite_port_override',
               default='',
               help="Use this value to override the original incoming "
                    "request port number. This has higher priority than "
                    "url_rewrite_port_header."),
    cfg.StrOpt('url_rewrite_port_header',
               default='X-Forwarded-Port',
               help="The HTTP Header that will be used to determine what the "
                    "original request port number was, even if it was "
                    "removed by an SSL terminator proxy."),
    cfg.StrOpt('url_rewrite_host_override',
               default='',
               help="Use this value to override the original incoming "
                    "request host name/IP address. This has higher priority "
                    "than url_rewrite_host_header."),
    cfg.StrOpt('url_rewrite_host_header',
               default='X-Forwarded-Host',
               help="The HTTP Header that will be used to determine what the "
                    "original request host name/IP address was, even if it "
                    "was removed by an SSL terminator proxy.")
]
cfg.CONF.register_opts(url_rewriter_middleware_opts)


class UrlRewriterMiddleware(wsgi.Middleware):
    """
    UrlRewriterMiddleware checks the configured value of
    url_rewrite_protocol_override, and replaces the wsgi.url_scheme
    environment variable with the above value.

    If url_rewrite_protocol_override is not configured, the HTTP header
    configured in url_rewrite_protocol_header will be used if it exists in the
    incoming request.

    Similarly, url_rewrite_port_override is used to override the port
    number of incoming request, and url_rewrite_port_header is used to
    configure the port number http header SERVER_PORT.

    Also, url_rewrite_host_override is used to override the host name or
    IP address of incoming request, and url_rewrite_host_header is used
    to configure the host name/IP address http header HTTP_HOST.

    This is useful if the server is behind an SSL termination proxy.
    """

    WSGI_URL_SCHEME = 'wsgi.url_scheme'
    SERVER_PORT = 'SERVER_PORT'
    HTTP_HOST = 'HTTP_HOST'

    def __init__(self, app, options):
        self.options = options
        LOG.info("Initialized url_rewriter middleware")
        super(UrlRewriterMiddleware, self).__init__(app)

        self.url_rewrite_protocol_override = (
            cfg.CONF.url_rewrite_protocol_override)
        self.url_rewrite_protocol_header = 'HTTP_{0}'.format(
            cfg.CONF.url_rewrite_protocol_header.upper().replace('-', '_'))

        self.url_rewrite_port_override = (
            cfg.CONF.url_rewrite_port_override)
        self.url_rewrite_port_header = 'HTTP_{0}'.format(
            cfg.CONF.url_rewrite_port_header.upper().replace('-', '_'))

        self.url_rewrite_host_override = (
            cfg.CONF.url_rewrite_host_override)
        self.url_rewrite_host_header = 'HTTP_{0}'.format(
            cfg.CONF.url_rewrite_host_header.upper().replace('-', '_'))

    def process_request(self, req):
        req.environ[self.WSGI_URL_SCHEME] = (
            self.url_rewrite_protocol_override or
            req.environ.get(
                self.url_rewrite_protocol_header,
                req.environ[self.WSGI_URL_SCHEME]))

        req.environ[self.SERVER_PORT] = (
            self.url_rewrite_port_override or
            req.environ.get(
                self.url_rewrite_port_header,
                req.environ[self.SERVER_PORT]))

        req.environ[self.HTTP_HOST] = (
            self.url_rewrite_host_override or
            req.environ.get(
                self.url_rewrite_host_header,
                req.environ[self.HTTP_HOST]))

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
