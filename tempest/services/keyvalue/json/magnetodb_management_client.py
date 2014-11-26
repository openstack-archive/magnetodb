# Copyright 2014 Mirantis Inc.
# All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from tempest.common import rest_client
from tempest import config


CONF = config.TempestConfig()


class MagnetoDBManagementClientJSON(rest_client.RestClient):

    def __init__(self, config, user, password, auth_url, tenant_name=None,
                 auth_version='v2'):

        super(MagnetoDBManagementClientJSON, self).__init__(
            config, user, password, auth_url, tenant_name, auth_version)

        self.service = CONF.magnetodb_management.service_type

    def create_backup(self, table_name, backup_name):
        url = '/'.join([self.tenant_id, table_name, 'backups'])
        request_body = "{{\"backup_name\": \"{}\"}}".format(backup_name)
        resp, body = self.post(url, request_body, self.headers)
        return resp, self._parse_resp(body)

    def describe_backup(self, table_name, backup_id):
        url = '/'.join([self.tenant_id, table_name, 'backups', backup_id])
        resp, body = self.get(url, self.headers)
        return resp, self._parse_resp(body)

    def list_backups(self, table_name, limit=None,
                     exclusive_start_backup_id=None):
        url = '/'.join([self.tenant_id, table_name, 'backups'])

        add_url = ''

        if limit:
            add_url = '?limit=%s' % limit
        if exclusive_start_backup_id:
            divider = '&' if add_url else '?'
            add_url += (divider + 'exclusive_start_backup_id=%s' %
                        exclusive_start_backup_id)
        url += add_url

        resp, body = self.get(url, self.headers)
        return resp, self._parse_resp(body)

    def delete_backup(self, table_name, backup_id):
        url = '/'.join([self.tenant_id, table_name, 'backups', backup_id])
        resp, body = self.delete(url, self.headers)
        return resp, self._parse_resp(body)

    def create_restore_job(self, table_name, backup_id):
        url = '/'.join([self.tenant_id, table_name, 'restores'])
        request_body = "{{\"backup_id\": \"{}\"}}".format(backup_id)
        resp, body = self.post(url, request_body, self.headers)
        return resp, self._parse_resp(body)

    def describe_restore_job(self, table_name, restore_job_id):
        url = '/'.join([self.tenant_id, table_name,
                        'restores', restore_job_id])
        resp, body = self.get(url, self.headers)
        return resp, self._parse_resp(body)

    def list_restore_jobs(self, table_name, limit=None,
                          exclusive_start_restore_job_id=None):

        url = '/'.join([self.tenant_id, table_name, 'restores'])

        add_url = ''

        if limit:
            add_url = '?limit=%s' % limit
        if exclusive_start_restore_job_id:
            divider = '&' if add_url else '?'
            add_url += (divider + 'exclusive_start_restore_job_id=%s' %
                        exclusive_start_restore_job_id)
        url += add_url

        resp, body = self.get(url, self.headers)
        return resp, self._parse_resp(body)
