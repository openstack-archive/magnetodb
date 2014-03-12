import json

from tempest.common import rest_client
from tempest import config

CONF = config.TempestConfig()


class MagnetoDBClientJSON(rest_client.RestClient):

    base_url = 'v1/%(project_id)s/data'
    tables_base_url = '/'.join([base_url, 'tables'])

    def create_table(self, attr_def, table_name, schema, lsi_indexes=None):
        url = self.tables_base_url

        post_body = {'attribute_definitions': attr_def,
                     'name': table_name,
                     'key_schema': schema,
                     'local_secondary_indexes': lsi_indexes}
        for k, v in post_body:
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def update_table(self, table_name):
        pass

    def delete_table(self, table_name):
        url = '/'.join([self.tables_base_url, table_name])
        resp, body = self.delete(url)
        return resp, self._parse_resp(body)

    def list_tables(self):
        url = self.tables_base_url

        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def describe_table(self, table_name):
        url = '/'.join([self.tables_base_url, table_name])
        resp, body = self.get(url)
        return resp, self._parse_resp(body)

    def put_item(self, table_name, item, expected=None, time_to_live=None,
                 return_values=None):
        url = '/'.join([self.tables_base_url, table_name, 'put_item'])
        post_body = {'item': item,
                     'expected': expected,
                     'time_to_live': time_to_live,
                     'return_values': return_values,
                     }
        for k, v in post_body:
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def update_item(self, table_name, key, attribute_updates=None,
                    expected=None, time_to_live=None, return_values=None):
        url = '/'.join([self.tables_base_url, table_name, 'put_item'])
        post_body = {'key': key,
                     'expected': expected,
                     'time_to_live': time_to_live,
                     'return_values': return_values,
                     'attribute_updates': attribute_updates,
                     }
        for k, v in post_body:
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def delete_item(self, table_name, key, expected=None):
        url = '/'.join([self.tables_base_url, table_name, 'delete_item'])
        post_body = {'key': key,
                     'expected': expected}
        for k, v in post_body:
            if v is not None:
                post_body.update({k: v})
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def get_item(self, table_name, key, attributes_to_get=None,
                 consistent_read=None):
        url = '/'.join([self.tables_base_url, table_name, 'get_item'])
        post_body = {'key': key,
                     'attributes_to_get': attributes_to_get,
                     'consistent_read': consistent_read,
                     }
        for k, v in post_body:
            if v is not None:
                post_body.update({k: v})
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def query(self, table_name, attributes_to_get=None, consistent_read=None,
              exclusive_start_key=None, index_name=None, key_conditions=None,
              limit=None, scan_index_forward=None, select=None):
        url = '/'.join([self.tables_base_url, table_name, 'query'])
        post_body = {'attributes_to_get': attributes_to_get,
                     'consistent_read': consistent_read,
                     'exclusive_start_key': exclusive_start_key,
                     'index_name': index_name,
                     'key_conditions': key_conditions,
                     'limit': limit,
                     'scan_index_forward': scan_index_forward,
                     'select': select,
                     }
        for k, v in post_body:
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def scan(self, table_name, attributes_to_get=None,
             exclusive_start_key=None,
             limit=None, scan_filter=None, select=None,
             segment=None, total_segments=None):
        url = '/'.join([self.tables_base_url, table_name, 'scan'])
        post_body = {'attributes_to_get': attributes_to_get,
                     'exclusive_start_key': exclusive_start_key,
                     'limit': limit,
                     'scan_filter': scan_filter,
                     'select': select,
                     'segment': segment,
                     'total_segments': total_segments,
                     }
        for k, v in post_body:
            if v is None:
                del post_body[k]
        post_body = json.dumps(post_body)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def batch_get_item(self, request_items):
        url = '/'.join([self.base_url, 'batch_get_item'])
        post_body = json.dumps(request_items)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)

    def batch_write_item(self, request_items):
        url = '/'.join([self.base_url, 'batch_write_item'])
        post_body = json.dumps(request_items)
        resp, body = self.post(url, post_body)
        return resp, self._parse_resp(body)
