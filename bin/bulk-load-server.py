from gevent import pywsgi
import json

from magnetodb.storage.impl.cassandra_impl import CassandraStorageImpl


class Ctx:
    def __init__(self):
        self.tenant = 'default_tenant'


def hello_world(environ, start_response):

    storage = CassandraStorageImpl()
    schema = storage.describe_table(Ctx(), 'table')

    try:
        stream = environ['wsgi.input']
        for chunk in stream:
            map = json.loads(chunk)
            storage.bulk_put_item('default_tenant', 'table', schema, map)

    except Exception as e:
        print str(e)

    start_response('200 OK', [('Content-Type', 'text/html')])
    yield 'Done\n'

if __name__ == '__main__':
    server = pywsgi.WSGIServer(('localhost', 9999), hello_world)

    server.serve_forever()
