# Copyright 2014 Symantec Corporation
# Copyright 2013 OpenStack Foundation
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

import os
import subprocess
import time

from tempest import cli
from tempest import config_magnetodb as config
from oslo_log import log as logging

CONF = config.CONF

LOG = logging.getLogger(__name__)

TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), 'magnetodb_templates')
TABLE_NAME = 'cli_test_table'

file_map = {
    'table-create': 'table-create-request.json',
    'item-put': 'item-put-request.json',
    'item-get': 'item-get-request.json',
    'item-delete': 'item-delete-request.json',
    'query': 'query-request.json',
    'scan': 'scan-request.json',
    'batch-write': 'batch-write-request.json',
}

for k, v in file_map.iteritems():
    file_map[k] = os.path.join(TEMPLATES_DIR, v)


class MagnetoDBCLIBase(cli.ClientTestBase):

    @classmethod
    def setUpClass(cls):
        if (not CONF.service_available.magnetodb):
            msg = "Skipping all MagnetoDB cli tests "\
                  "because it is not available"
            raise cls.skipException(msg)
        super(MagnetoDBCLIBase, cls).setUpClass()


class SimpleReadOnlyMagnetoDBClientTest(MagnetoDBCLIBase):
    """Basic tests for MagnetoDB CLI client that does not create tables.
    """

    def test_magnetodb_fake_action(self):
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb('this-does-not-exist')
        expl = ar.exception.stderr
        self.assertIn('Unknown command', expl)

    def test_magnetodb_table_list(self):
        self.magnetodb('table-list')
        self.magnetodb('table-list', params='--limit 10')
        self.magnetodb('table-list', params='--start_table_name first')

    def test_magnetodb_describe_non_existent_table(self):
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb('table-describe', params='bad-table')
        expl = ar.exception.stderr
        self.assertIn('TableNotExistsException', expl)

    def _without_request_file(self, cmd, params='fake-table'):
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb(cmd, params=params)
        expl = ar.exception.stderr
        self.assertIn("'--request-file' is a required", expl)

    def test_magnetodb_table_create_without_request_file(self):
        self._without_request_file('table-create', '')

    def test_magnetodb_batch_write_without_request_file(self):
        self._without_request_file('batch-write', '')

    def test_magnetodb_item_put_without_request_file(self):
        self._without_request_file('item-put')

    def test_magnetodb_item_get_without_request_file(self):
        self._without_request_file('item-get')

    def test_magnetodb_item_delete_without_request_file(self):
        self._without_request_file('item-delete')

    def test_magnetodb_query_without_request_file(self):
        self._without_request_file('query')

    def test_magnetodb_scan_without_request_file(self):
        self._without_request_file('scan')

    def test_magnetodb_delete_non_existent_table(self):
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb('table-delete', params='bad-table')
        expl = ar.exception.stderr
        self.assertIn('TableNotExistsException', expl)

    def test_magnetodb_index_describe_non_existent_table(self):
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb('index-describe', params='bad-table index')
        expl = ar.exception.stderr
        self.assertIn('TableNotExistsException', expl)

    def test_magnetodb_index_list_non_existent_table(self):
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb('index-list %s', params='bad-table')
        expl = ar.exception.stderr
        self.assertIn('TableNotExistsException', expl)


class MagnetoDBClientTest(MagnetoDBCLIBase):
    """Tests that can create tables and manipulate data."""

    def tearDown(self):
        try:
            self.magnetodb('table-delete', params=TABLE_NAME)
        except Exception:
            pass
        else:
            while True:
                try:
                    self.magnetodb('table-describe', params=TABLE_NAME)
                except Exception:
                    break
                time.sleep(2)
        super(MagnetoDBClientTest, self).tearDown()

    def _create_table(self):
        cmd = 'table-create'
        params = '--request-file %s' % file_map[cmd]
        resp = self.magnetodb(cmd, params=params)
        self._wait_for_table_active()
        return resp

    def _wait_for_table_active(self):
        while True:
            resp = self.magnetodb('table-describe', params=TABLE_NAME)
            if 'ACTIVE' in resp:
                break
            time.sleep(2)

    def test_magnetodb_delete_table(self):
        self._create_table()
        resp = self.magnetodb('table-delete', params=TABLE_NAME)
        expected_resp = 'Deleted table: %s\n' % TABLE_NAME
        self.assertEqual(expected_resp, resp)

    def test_magnetodb_describe_table(self):
        self._create_table()
        resp = self.magnetodb('table-describe', params='cli_test_table')
        self.assertIn(TABLE_NAME, resp)
        self.assertIn('attribute_definitions', resp)

    def test_magnetodb_create_table(self):
        resp = self._create_table()
        self.assertIn(TABLE_NAME, resp)
        self.assertIn('attribute_definitions', resp)

    def test_magnetodb_list_table_with_creation(self):
        resp = self.magnetodb('table-list')
        self.assertNotIn(TABLE_NAME, resp)
        self._create_table()
        resp = self.magnetodb('table-list')
        self.assertIn(TABLE_NAME, resp)
        self.assertIn('Table Name', resp)

    def test_magnetodb_index_list(self):
        self._create_table()
        resp = self.magnetodb('index-list', params=TABLE_NAME)
        self.assertIn('index1', resp)
        self.assertIn('index2', resp)

    def test_magnetodb_index_describe(self):
        self._create_table()
        params = ' '.join([TABLE_NAME, 'index1'])
        resp = self.magnetodb('index-describe', params=params)
        self.assertIn('index1', resp)
        self.assertIn('key_schema', resp)

    def test_magnetodb_index_describe_non_existent_index(self):
        self._create_table()
        params = ' '.join([TABLE_NAME, 'bad-index'])
        with self.assertRaises(subprocess.CalledProcessError) as ar:
            self.magnetodb('index-describe', params=params)
        expl = ar.exception.stderr
        self.assertIn('Index "bad-index" is not found', expl)

    def test_magnetodb_item_put(self):
        self._create_table()
        cmd = 'item-put'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        resp = self.magnetodb(cmd, params=params)
        self.assertIn('hash_attr', resp)

    def test_magnetodb_item_get(self):
        self._create_table()
        cmd = 'item-put'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        self.magnetodb(cmd, params=params)
        cmd = 'item-get'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        resp = self.magnetodb(cmd, params=params)
        self.assertIn('hash_attr', resp)
        self.assertIn('hash_value', resp)

    def test_magnetodb_item_get_non_existent_item(self):
        self._create_table()
        cmd = 'item-get'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        resp = self.magnetodb(cmd, params=params)
        self.assertNotIn('hash_attr', resp)

    def test_magnetodb_item_delete(self):
        self._create_table()
        cmd = 'item-put'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        self.magnetodb(cmd, params=params)

        get_cmd = 'item-get'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[get_cmd])
        resp = self.magnetodb(get_cmd, params=params)
        self.assertIn('hash_attr', resp)

        resp = self.magnetodb('item-delete', params=params)
        self.assertNotIn('hash_attr', resp)

        resp = self.magnetodb(get_cmd, params=params)
        self.assertNotIn('hash_attr', resp)

    def test_magnetodb_query(self):
        self._create_table()

        cmd = 'query'
        query_params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        resp = self.magnetodb(cmd, params=query_params)
        self.assertEqual('\n', resp)

        put_cmd = 'item-put'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[put_cmd])
        self.magnetodb(put_cmd, params=params)

        resp = self.magnetodb(cmd, params=query_params)
        self.assertIn('hash_attr', resp)
        self.assertIn('hash_value', resp)

    def test_magnetodb_scan(self):
        self._create_table()

        cmd = 'scan'
        scan_params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        resp = self.magnetodb(cmd, params=scan_params)
        self.assertEqual('\n', resp)

        put_cmd = 'item-put'
        put_params = '%s --request-file %s' % (TABLE_NAME, file_map[put_cmd])
        self.magnetodb(put_cmd, params=put_params)

        resp = self.magnetodb(cmd, params=scan_params)
        self.assertIn('hash_attr', resp)
        self.assertIn('hash_value', resp)

    def test_magnetodb_batch_write(self):
        self._create_table()

        cmd = 'batch-write'
        bw_params = '--request-file %s' % file_map[cmd]
        resp = self.magnetodb(cmd, params=bw_params)
        self.assertEqual('\n', resp)

        cmd = 'item-get'
        params = '%s --request-file %s' % (TABLE_NAME, file_map[cmd])
        resp = self.magnetodb(cmd, params=params)
        self.assertIn('hash_attr', resp)
        self.assertIn('hash_value', resp)
