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

import json

from tempest.common import rest_client
from tempest import config


CONF = config.TempestConfig()


class MagnetoDBClientJSON(rest_client.RestClient):
    def create_table(self, attr_def, table_name, schema, lsi_indexes=None):
        post_body = {'attribute_definitions': attr_def,
                     'table_name': table_name,
                     'key_schema': schema,
                     'local_secondary_indexes': lsi_indexes}
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post('data/tables', post_body, self.headers)
        return resp, self._parse_resp(body)

    def update_table(self, table_name):
        pass

    def delete_table(self, table_name):
        url = '/'.join(['data/tables', table_name])
        resp, body = self.delete(url, self.headers)
        return resp, self._parse_resp(body)

    def list_tables(self):
        resp, body = self.get('data/tables', self.headers)
        return resp, self._parse_resp(body)

    def describe_table(self, table_name):
        url = '/'.join(['data/tables', table_name])
        resp, body = self.get(url, self.headers)
        return resp, self._parse_resp(body)

    def put_item(self, table_name, item, expected=None, time_to_live=None,
                 return_values=None):
        url = '/'.join(['data/tables', table_name, 'put_item'])
        post_body = {'item': item,
                     'expected': expected,
                     'time_to_live': time_to_live,
                     'return_values': return_values,
                     }
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body, self.headers)
        return resp, self._parse_resp(body)

    def update_item(self, table_name, key, attribute_updates=None,
                    expected=None, time_to_live=None, return_values=None):
        url = '/'.join(['data/tables', table_name, 'put_item'])
        post_body = {'key': key,
                     'expected': expected,
                     'time_to_live': time_to_live,
                     'return_values': return_values,
                     'attribute_updates': attribute_updates,
                     }
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body, self.headers)
        return resp, self._parse_resp(body)

    def delete_item(self, table_name, key, expected=None):
        url = '/'.join(['data/tables', table_name, 'delete_item'])
        post_body = {'key': key,
                     'expected': expected}
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body, self.headers)
        return resp, self._parse_resp(body)

    def get_item(self, table_name, key, attributes_to_get=None,
                 consistent_read=None):
        url = '/'.join(['data/tables', table_name, 'get_item'])
        post_body = {'key': key,
                     'attributes_to_get': attributes_to_get,
                     'consistent_read': consistent_read,
                     }
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body, self.headers)
        return resp, self._parse_resp(body)

    def query(self, table_name, attributes_to_get=None, consistent_read=None,
              exclusive_start_key=None, index_name=None, key_conditions=None,
              limit=None, scan_index_forward=None, select=None):
        url = '/'.join(['data/tables', table_name, 'query'])
        post_body = {'attributes_to_get': attributes_to_get,
                     'consistent_read': consistent_read,
                     'exclusive_start_key': exclusive_start_key,
                     'index_name': index_name,
                     'key_conditions': key_conditions,
                     'limit': limit,
                     'scan_index_forward': scan_index_forward,
                     'select': select,
                     }
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body, self.headers)
        return resp, self._parse_resp(body)

    def scan(self, table_name, attributes_to_get=None,
             exclusive_start_key=None,
             limit=None, scan_filter=None, select=None,
             segment=None, total_segments=None):
        url = '/'.join(['data/tables', table_name, 'scan'])
        post_body = {'attributes_to_get': attributes_to_get,
                     'exclusive_start_key': exclusive_start_key,
                     'limit': limit,
                     'scan_filter': scan_filter,
                     'select': select,
                     'segment': segment,
                     'total_segments': total_segments,
                     }
        for k, v in post_body.items():
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body, self.headers)
        return resp, self._parse_resp(body)

    def batch_get_item(self, request_items):
        post_body = json.dumps(request_items)
        resp, body = self.post('data/batch_get_item', post_body, self.headers)
        return resp, self._parse_resp(body)

    def batch_write_item(self, request_items):
        post_body = json.dumps(request_items)
        resp, body = self.post('data/batch_write_item', post_body,
                               self.headers)
        return resp, self._parse_resp(body)
