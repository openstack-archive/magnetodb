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

from oslo.config import cfg

from magnetodb.common import wsgi
from magnetodb.openstack.common import log as logging

WSGI_URL_SCHEME = 'wsgi.url_scheme'
SERVER_PORT = 'SERVER_PORT'
HTTP_HOST = 'HTTP_HOST'

LOG = logging.getLogger(__name__)

ssl_proxy_middleware_opts = [
    cfg.StrOpt('secure_proxy_ssl_protocol_override',
               default='',
               help="Use this value to override the original incoming "
                    "request protocol scheme. This has higher priority than "
                    "secure_proxy_ssl_header."),
    cfg.StrOpt('secure_proxy_ssl_header',
               default='X-Forwarded-Proto',
               help="The HTTP Header that will be used to determine which "
                    "the original request protocol scheme was, even if it"
                    "was removed by an SSL terminator proxy."),
    cfg.StrOpt('secure_proxy_ssl_port_override',
               default='',
               help="Use this value to override the original incoming "
                    "request port number. This has higher priority than "
                    "secure_proxy_ssl_port_header."),
    cfg.StrOpt('secure_proxy_ssl_port_header',
               default='X-Forwarded-Port',
               help="The HTTP Header that will be used to determine what the "
                    "original request port number was, even if it was "
                    "removed by an SSL terminator proxy."),
    cfg.StrOpt('secure_proxy_ssl_host_override',
               default='',
               help="Use this value to override the original incoming "
                    "request host name/IP address. This has higher priority "
                    "than secure_proxy_ssl_host_header."),
    cfg.StrOpt('secure_proxy_ssl_host_header',
               default='X-Forwarded-Host',
               help="The HTTP Header that will be used to determine what the "
                    "original request host name/IP address was, even if it "
                    "was removed by an SSL terminator proxy.")
]
cfg.CONF.register_opts(ssl_proxy_middleware_opts)


class SSLProxyMiddleware(wsgi.Middleware):
    """
    SSLProxyMiddleware checks the configured value of
    secure_proxy_ssl_protocol_override, and replaces the wsgi.url_scheme
    environment variable with the above value.

    If secure_proxy_ssl_protocol_override is not configured, the HTTP header
    configured in secure_proxy_ssl_header will be used if it exists in the
    incoming request.

    Similarly, secure_proxy_ssl_port_override is used to override the port
    number of incoming request, and secure_proxy_ssl_port_header is used to
    configure the port number http header SERVER_PORT.

    Also, secure_proxy_ssl_host_override is used to override the host name or
    IP address of incoming request, and secure_proxy_ssl_host_header is used
    to configure the host name/IP address http header HTTP_HOST.

    This is useful if the server is behind an SSL termination proxy.
    """
    def __init__(self, app, options):
        self.options = options
        LOG.info("Initialized ssl_proxy middleware")
        super(SSLProxyMiddleware, self).__init__(app)

        self.secure_proxy_ssl_protocol_override = (
            cfg.CONF.secure_proxy_ssl_protocol_override)
        self.secure_proxy_ssl_header = 'HTTP_{0}'.format(
            cfg.CONF.secure_proxy_ssl_header.upper().replace('-', '_'))

        self.secure_proxy_ssl_port_override = (
            cfg.CONF.secure_proxy_ssl_port_override)
        self.secure_proxy_ssl_port_header = 'HTTP_{0}'.format(
            cfg.CONF.secure_proxy_ssl_port_header.upper().replace('-', '_'))

        self.secure_proxy_ssl_host_override = (
            cfg.CONF.secure_proxy_ssl_host_override)
        self.secure_proxy_ssl_host_header = 'HTTP_{0}'.format(
            cfg.CONF.secure_proxy_ssl_host_header.upper().replace('-', '_'))

    def process_request(self, req):
        req.environ[WSGI_URL_SCHEME] = (
            self.secure_proxy_ssl_protocol_override or
            req.environ.get(
                self.secure_proxy_ssl_header,
                req.environ[WSGI_URL_SCHEME]))

        req.environ[SERVER_PORT] = (
            self.secure_proxy_ssl_port_override or
            req.environ.get(
                self.secure_proxy_ssl_port_header,
                req.environ[SERVER_PORT]))

        req.environ[HTTP_HOST] = (
            self.secure_proxy_ssl_host_override or
            req.environ.get(
                self.secure_proxy_ssl_host_header,
                req.environ[HTTP_HOST]))

    @classmethod
    def factory_method(cls, global_config, **local_config):
        return lambda application: cls(application, local_config)
