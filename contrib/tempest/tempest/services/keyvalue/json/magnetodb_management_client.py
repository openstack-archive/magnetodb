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

from oslo_serialization import jsonutils as json

from tempest.common import service_client
from tempest import config_magnetodb as config

CONF = config.CONF
service_client.CONF = CONF


class MagnetoDBManagementClientJSON(service_client.ServiceClient):

    def create_backup(self, table_name, backup_name, strategy={}):
        url = '{}/tables/{}/backups'.format(self.tenant_id, table_name)
        request_body = (
            "{{\"backup_name\": \"{}\", \"strategy\":{}}}".format(
                backup_name, json.dumps(strategy))
        )
        resp, body = self.post(url, request_body)
        return resp, self._parse_resp(body)

    def describe_backup(self, table_name, backup_id):
        url = '{}/tables/{}/backups/{}'.format(
            self.tenant_id, table_name, backup_id
        )
        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def list_backups(self, table_name, limit=None,
                     exclusive_start_backup_id=None):
        url = '{}/tables/{}/backups'.format(self.tenant_id, table_name)

        add_url = ''

        if limit:
            add_url = '?limit=%s' % limit
        if exclusive_start_backup_id:
            divider = '&' if add_url else '?'
            add_url += (divider + 'exclusive_start_backup_id=%s' %
                        exclusive_start_backup_id)
        url += add_url

        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def delete_backup(self, table_name, backup_id):
        url = '{}/tables/{}/backups/{}'.format(
            self.tenant_id, table_name, backup_id
        )
        resp, body = self.delete(url)
        return resp, self._parse_resp(body)

    def create_restore_job(self, table_name, backup_id):
        url = '{}/tables/{}/restores'.format(self.tenant_id, table_name)
        request_body = "{{\"backup_id\": \"{}\"}}".format(backup_id)
        resp, body = self.post(url, request_body)
        return resp, self._parse_resp(body)

    def describe_restore_job(self, table_name, restore_job_id):
        url = '{}/tables/{}/restores/{}'.format(
            self.tenant_id, table_name, restore_job_id
        )

        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def list_restore_jobs(self, table_name, limit=None,
                          exclusive_start_restore_job_id=None):

        url = '{}/tables/{}/restores'.format(self.tenant_id, table_name)

        add_url = ''

        if limit:
            add_url = '?limit=%s' % limit
        if exclusive_start_restore_job_id:
            divider = '&' if add_url else '?'
            add_url += (divider + 'exclusive_start_restore_job_id=%s' %
                        exclusive_start_restore_job_id)
        url += add_url

        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def _parse_resp(self, body):
        return json.loads(body)
