import base64
import binascii
from gevent import pywsgi
import ujson as json
import Queue

from magnetodb.common import exception
from magnetodb.storage import models
import magnetodb.storage.impl.cassandra_impl as impl


keyspace_name = 'default_tenant'
table_name = 'table'
contact_points = ('127.0.0.1',)
storage = impl.CassandraStorageImpl(contact_points=contact_points)


USER_PREFIX = 'user_'


class Ctx:
    def __init__(self):
        self.tenant = keyspace_name


context = Ctx()


API_TO_CASSANDRA_TYPES = {
    'S': 'text',
    'N': 'decimal',
    'B': 'blob',
    'SS': 'set<text>',
    'NS': 'set<decimal>',
    'BS': 'set<blob>'
}


def parse_attr_val(json):
    type, val = json.iteritems().next()

    if type == 'S':
        attr_val = val
    elif type == 'N':
        # attr_val = decimal.Decimal(val)
        attr_val = val
    elif type == 'B':
        attr_val = base64.decodestring(val)
    elif type == 'SS':
        attr_val = val
    elif type == 'NS':
        attr_val = [
            # decimal.Decimal(v)
            int(v)
            for v in val
        ]
    elif type == 'BS':
        attr_val = [
            base64.decodestring(v)
            for v in val
        ]

    return type, attr_val


def parse_attribute_map(json):
    return {
        name: parse_attr_val(val)
        for name, val
        in json.iteritems()
    }

CREATE_SYSTEM_KEYSPACE = """
    CREATE KEYSPACE magnetodb
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1 };
    """

CREATE_SYSTEM_TABLE = """
    CREATE TABLE magnetodb.table_info(
        tenant text,
        name text,
        exists int,
        "schema" text,
        status text,
        internal_name text,
        PRIMARY KEY(tenant, name));
    """

CREATE_USER_KEYSPACE = """
    CREATE KEYSPACE user_default_tenant
    WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 1 };
    """


def init_test_env():
    try:
        storage.session.execute(CREATE_SYSTEM_KEYSPACE)
    except Exception as e:
        print str(e)

    try:
        storage.session.execute(CREATE_SYSTEM_TABLE)
    except Exception as e:
        print str(e)

    try:
        storage.session.execute(CREATE_USER_KEYSPACE)
    except Exception as e:
        print str(e)

    attrs = {
        'id': models.ATTRIBUTE_TYPE_STRING
    }

    schema = models.TableSchema(attrs, ['id'])

    try:
        storage.create_table(context, table_name, schema)
    except exception.TableAlreadyExistsException:
        storage.session.execute('TRUNCATE {}.{}'.format(
            impl.USER_PREFIX + keyspace_name,
            impl.USER_PREFIX + table_name))


def _encode_predefined_attr_value(attr_type, attr_value):
    if attr_value is None:
        return 'null'
    if attr_type in ('SS', 'NS', 'BS'):
        values = ','.join([
            _encode_single_value_as_predefined_attr(
                attr_type[0], v)
            for v in attr_value
        ])
        return '{{{}}}'.format(values)
    else:
        return _encode_single_value_as_predefined_attr(
            attr_type[0], attr_value
        )


def _encode_single_value_as_predefined_attr(attr_type, attr_value):
    if attr_type == 'S':
        return "'{}'".format(attr_value)
    elif attr_type == 'N':
        return attr_value
    elif attr_type == 'B':
        return "0x{}".format(binascii.hexlify(attr_value))
    else:
        assert False, "Value wasn't formatted for cql query {}:{}".format(
            attr_type, attr_value)


def _encode_dynamic_attr_value(attr_type, attr_value):
    if attr_value is None:
        return 'null'
    return "0x{}".format(binascii.hexlify(json.dumps(attr_value)))


def _encode_single_value_as_dynamic_attr(attr_type, attr_value):
    if attr_type == 'S':
        return attr_value
    elif attr_type == 'N':
        return attr_value
    elif attr_type == 'B':
        return attr_value
    else:
        assert False, "Value wasn't formatted for cql query {}:{}".format(
            attr_type, attr_value)


def _append_types_system_attr_value(table_schema, attribute_map,
                                    query_builder=None, prefix=""):
    if query_builder is None:
        query_builder = []
    query_builder.append(prefix)
    prefix = ""
    query_builder.append("{")
    for attr, (typ, val) in attribute_map.iteritems():
        if attr not in table_schema.attribute_type_map:
            query_builder += (
                prefix, "'", attr, "':'",
                API_TO_CASSANDRA_TYPES[typ], "'"
            )
            prefix = ","
    query_builder.append("}")
    return query_builder


def _append_exists_system_attr_value(attribute_map, query_builder=None,
                                     prefix=""):
    if query_builder is None:
        query_builder = []
    query_builder.append(prefix)
    prefix = ""
    query_builder.append("{")
    for attr, _ in attribute_map.iteritems():
        query_builder += (prefix, "'", attr, "'")
        prefix = ","
    query_builder.append("}")
    return query_builder


def _append_insert_query(table_info, attribute_map):

    query_builder = []

    query_builder += (
        'INSERT INTO "', USER_PREFIX, table_info.tenant, '"."',
        table_info.internal_name, '" ('
    )
    attr_values = []
    dynamic_attr_names = []
    dynamic_attr_values = []
    map_keys = table_info.schema.attribute_type_map.keys()

    for name, (typ, val) in attribute_map.iteritems():
        if name in map_keys:
            query_builder += (
                '"', USER_PREFIX, name, '",'
            )
            attr_values.append(_encode_predefined_attr_value(typ, val))
        else:
            dynamic_attr_names.append(name)
            dynamic_attr_values.append(
                _encode_dynamic_attr_value(typ, val)
            )

    query_builder += (
        impl.SYSTEM_COLUMN_EXTRA_ATTR_DATA, ",",
        impl.SYSTEM_COLUMN_EXTRA_ATTR_TYPES, ",",
        impl.SYSTEM_COLUMN_ATTR_EXIST,
        ") VALUES("
    )

    for attr_value in attr_values:
        query_builder += (
            attr_value, ","
        )

    query_builder.append("{")

    if dynamic_attr_values:
        dynamic_value_iter = iter(dynamic_attr_values)
        for name in dynamic_attr_names:
            query_builder += (
                "'", name, "':" + dynamic_value_iter.next(), ","
            )
        query_builder.pop()

    query_builder.append("},")
    _append_types_system_attr_value(table_info.schema, attribute_map,
                                    query_builder)
    _append_exists_system_attr_value(attribute_map, query_builder,
                                     prefix=",")
    query_builder.append(")")

    return query_builder


def put_item_async(table_name, attribute_map):
    table_info = storage._get_table_info(context, table_name)

    query_builder = _append_insert_query(
        table_info, attribute_map
    )

    query = "".join(query_builder)

    return storage.session.execute_async(query)


def put_item_app(environ, start_response):

    queue_size = 1000
    futures = Queue.Queue(maxsize=queue_size + 1)
    count = 0

    print 'Request'

    try:
        stream = environ['wsgi.input']
        for chunk in stream:
            data = json.loads(chunk)

            if count % 10000 == 0:
                print count

            count += 1

            if count >= queue_size:
                try:
                    old_future = futures.get_nowait()
                    old_future.result()
                except Exception as e:
                    print str(e)

            try:
                attr_map = parse_attribute_map(data)

                future = put_item_async(table_name, attr_map)
                futures.put_nowait(future)
            except Exception as e:
                print str(e)

    except Exception as e:
        print str(e)

    while not futures.empty():
        f = futures.get_nowait()
        f.result()

    start_response('200 OK', [('Content-Type', 'text/html')])
    yield 'Done\n'

if __name__ == '__main__':
    server = pywsgi.WSGIServer(('localhost', 9999), put_item_app)

    server.serve_forever()
