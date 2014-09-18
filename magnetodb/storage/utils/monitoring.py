import json

from urllib2 import Request
from urllib2 import urlopen


class Monitoring:

    def __init__(self, url, **kwargs):
        self.url = url
        self.data = None
        self.req_config = {}

        self.timeout = kwargs.get('timeout', 10)

    def request(self, type, **kwargs):
        if not isinstance(self.data, dict):
            self.data = {}
        self.data = self._mkrequest(type, **kwargs)
        response = self._get_json()
        return response

    def add_request(self, type, **kwargs):
        new_response = self._mkrequest(type, **kwargs)
        if not isinstance(self.data, list):
            self.data = list()
        self.data.append(new_response)

    def get_requests(self):
        response = self._get_json()
        return response

    def _get_json(self):
        if isinstance(self.data, dict):
            main_request = self.data.copy()
        else:
            main_request = []
            for request in self.data:
                request = request.copy()
                main_request.append(request)

        j_data = json.dumps(main_request).encode('utf-8')

        try:
            request = Request(self.url, j_data,
                              {'content-type': 'application/json'})

            response_stream = urlopen(request, timeout=self.timeout)
            json_data = response_stream.read()
        except Exception as e:
            raise JmxError('Could not connect. Got error %s' % e)

        try:
            python_dict = json.loads(json_data.decode())
        except:
            raise JmxError("Could not decode into json. \
                    Is Jolokia running at %s" % self.url)
        return python_dict

    def _mkrequest(self, type, **kwargs):
        new_request = {'type': type, 'config': self.req_config}

        if type != 'list':
            new_request['mbean'] = kwargs.get('mbean')
        else:
            new_request['path'] = kwargs.get('path')

        if type == 'read':
            new_request['attribute'] = kwargs.get('attribute')
            new_request['path'] = kwargs.get('path')

        elif type == 'exec':
            new_request['operation'] = kwargs.get('operation')
            new_request['arguments'] = kwargs.get('arguments')

        return new_request


class JmxError(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message
