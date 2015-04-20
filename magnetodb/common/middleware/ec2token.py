# Copyright 2014 Mirantis Inc.
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

import hashlib
import requests

from oslo_config import cfg
from oslo_serialization import jsonutils as json

from magnetodb.api.amz import exception as amz_exception
from oslo_middleware import base as wsgi
from magnetodb.i18n import _, _LE, _LI
from magnetodb.openstack.common import log as logging


LOG = logging.getLogger(__name__)


opts = [
    cfg.StrOpt('auth_uri',
               default='',
               help=_("Authentication Endpoint URI.")),
    cfg.BoolOpt('multi_cloud',
                default=False,
                help=_('Allow orchestration of multiple clouds.')),
    cfg.ListOpt('allowed_auth_uris',
                default=[],
                help=_('Allowed keystone endpoints for auth_uri when '
                       'multi_cloud is enabled. At least one endpoint needs '
                       'to be specified.'))
]
cfg.CONF.register_opts(opts, group='ec2authtoken')


class EC2Token(wsgi.Middleware):
    """Authenticate an EC2 request with keystone and convert to token."""

    def __init__(self, app, conf):
        self.conf = conf
        super(EC2Token, self).__init__(app)

    def _conf_get(self, name):
        return self.conf.get(name, None)

    def _conf_get_auth_uri(self):
        return self._conf_get('auth_uri')

    @staticmethod
    def _conf_get_keystone_ec2_uri(auth_uri):
        if auth_uri.endswith('/'):
            return '%sec2tokens' % auth_uri
        return '%s/ec2tokens' % auth_uri

    def _get_signature(self, req):
        """
        Extract the signature from the request, this can be a get/post
        variable or for v4 also in a header called 'Authorization'
        - params['Signature'] == version 0,1,2,3
        - params['X-Amz-Signature'] == version 4
        - header 'Authorization' == version 4
        """
        sig = req.params.get('Signature') or req.params.get('X-Amz-Signature')

        if sig is None and 'Authorization' in req.headers:
            auth_str = req.headers['Authorization']
            sig = auth_str.partition("Signature=")[2].split(',')[0]

        return sig

    def _get_access(self, req):
        """
        Extract the access key identifier, for v 0/1/2/3 this is passed
        as the AccessKeyId parameter, for version4 it is either and
        X-Amz-Credential parameter or a Credential= field in the
        'Authorization' header string
        """
        access = req.params.get('AWSAccessKeyId')
        if access is None:
            cred_param = req.params.get('X-Amz-Credential')
            if cred_param:
                access = cred_param.split("/")[0]

        if access is None and 'Authorization' in req.headers:
            auth_str = req.headers['Authorization']
            cred_str = auth_str.partition("Credential=")[2].split(',')[0]
            access = cred_str.split("/")[0]

        return access

    def process_request(self, req):
        if not self._conf_get('multi_cloud'):
            return self._authorize(req, self._conf_get_auth_uri())
        else:
            # attempt to authorize for each configured allowed_auth_uris
            # until one is successful.
            # This is safe for the following reasons:
            # 1. AWSAccessKeyId is a randomly generated sequence
            # 2. No secret is transferred to validate a request
            last_failure = None
            for auth_uri in self._conf_get('allowed_auth_uris'):
                try:
                    LOG.debug("Attempt authorize on %s", auth_uri)
                    return self._authorize(req, auth_uri)
                except amz_exception.AWSErrorResponseException as e:
                    LOG.debug("Authorize failed: %s", e.__class__)
                    last_failure = e
            raise last_failure or amz_exception.AWSAccessDeniedError()

    def _is_v2_token(self, token_body):
        return 'access' in token_body

    def _is_v3_token(self, token_body):
        return 'token' in token_body

    def _authorize(self, req, auth_uri):
        # Read request signature and access id.
        # If we find X-Auth-User in the headers we ignore a key error
        # here so that we can use both authentication methods.
        # Returning here just means the user didn't supply AWS
        # authentication and we'll let the app try native keystone next.
        LOG.info(_LI("Checking AWS credentials.."))

        signature = self._get_signature(req)
        if not signature:
            if 'X-Auth-User' in req.headers:
                return self.application
            elif 'X-Auth-Token' in req.headers:
                return self.application
            else:
                LOG.info(_LI("No AWS Signature found."))
                raise amz_exception.AWSIncompleteSignatureError()

        access = self._get_access(req)
        if not access:
            if 'X-Auth-User' in req.headers:
                return self.application
            else:
                LOG.info(_LI("No AWSAccessKeyId/Authorization Credential"))
                raise amz_exception.AWSIncompleteSignatureError()

        LOG.info(_LI("AWS credentials found, checking against keystone."))

        if not auth_uri:
            LOG.error(_LE("Ec2Token authorization failed, no auth_uri "
                          "specified in config file"))
            raise amz_exception.AWSErrorResponseException(
                _LI('Service misconfigured')
            )
        # Make a copy of args for authentication and signature verification.
        auth_params = dict(req.params)
        # 'Signature' param Not part of authentication args
        auth_params.pop('Signature', None)

        # Authenticate the request.
        # AWS v4 authentication requires a hash of the body
        body_hash = hashlib.sha256(req.body).hexdigest()
        creds = {'ec2Credentials': {'access': access,
                                    'signature': signature,
                                    'host': req.host,
                                    'verb': req.method,
                                    'path': req.path,
                                    'params': auth_params,
                                    'headers': req.headers,
                                    'body_hash': body_hash
                                    }}
        creds_json = json.dumps(creds)
        headers = {'Content-Type': 'application/json'}

        keystone_ec2_uri = self._conf_get_keystone_ec2_uri(auth_uri)
        LOG.info(_LI('Authenticating with %s'), keystone_ec2_uri)
        response = requests.post(keystone_ec2_uri, data=creds_json,
                                 headers=headers)
        result = response.json()
        try:
            if self._is_v2_token(result):
                token_id = result['access']['token']['id']
                tenant = result['access']['token']['tenant']['name']
                tenant_id = result['access']['token']['tenant']['id']
                metadata = result['access'].get('metadata', {})
                roles = metadata.get('roles', [])
            elif self._is_v3_token(result):
                token_id = response.headers['X-Subject-Token']
                tenant = result['token']['project']['name']
                tenant_id = result['token']['project']['id']
                metadata = result['token']['roles']
                roles = [r['name'] for r in metadata]
            else:
                raise amz_exception.AWSInvalidClientTokenIdError()
            LOG.info(_LI("AWS authentication successful."))
        except (AttributeError, KeyError):
            LOG.info(_LI("AWS authentication failure."))
            # Try to extract the reason for failure so we can return the
            # appropriate AWS error via raising an exception
            try:
                reason = result['error']['message']
            except KeyError:
                reason = None

            if reason == "EC2 access key not found.":
                raise amz_exception.AWSInvalidClientTokenIdError()
            elif reason == "EC2 signature not supplied.":
                raise amz_exception.AWSSignatureError()
            else:
                raise amz_exception.AWSAccessDeniedError()

        # Authenticated!
        ec2_creds = {'ec2Credentials': {'access': access,
                                        'signature': signature}}
        req.headers['X-Auth-EC2-Creds'] = json.dumps(ec2_creds)
        req.headers['X-Auth-Token'] = token_id
        req.headers['X-Tenant-Name'] = tenant
        req.headers['X-Tenant-Id'] = tenant_id
        req.headers['X-Auth-URL'] = auth_uri

        req.headers['X-Roles'] = ','.join(roles)

        return self.application


def EC2Token_filter_factory(global_conf, **local_conf):
    """
    Factory method for paste.deploy
    """
    conf = global_conf.copy()
    conf.update(local_conf)

    def filter(app):
        return EC2Token(app, conf)

    return filter
