import base64
import binascii
import Queue
import re
import shlex
import string

import ujson as json

from magnetodb import common
from magnetodb.common import exception
from magnetodb.openstack.common import log as logging
from magnetodb.storage import models
import magnetodb.storage.impl.cassandra_impl as impl


DEFAULT_INDEX_NAME = ''
DEFAULT_INDEX_VALUE_STRING = ''
DEFAULT_INDEX_VALUE_NUMBER = 0
DEFAULT_INDEX_VALUE_BLOB = '0x'

DEFAULT_INDEX_NAME_QUOTED = "''"
DEFAULT_INDEX_VALUE_STRING_QUOTED = "''"
DEFAULT_INDEX_VALUE_NUMBER_QUOTED = "0"
DEFAULT_INDEX_VALUE_BLOB_QUOTED = "0x"

LOG = logging.getLogger(__name__)

USER_PREFIX = 'user_'


class Ctx:
    def __init__(self, tenant):
        self.tenant = tenant


API_TO_CASSANDRA_TYPES = {
    'S': 'text',
    'N': 'decimal',
    'B': 'blob',
    'SS': 'set<text>',
    'NS': 'set<decimal>',
    'BS': 'set<blob>'
}

API_TO_QUOTED_CASSANDRA_TYPES = {
    'S': "'text'",
    'N': "'decimal'",
    'B': "'blob'",
    'SS': "'set<text>'",
    'NS': "'set<decimal>'",
    'BS': "'set<blob>'"
}


INDEX_ATTRS = {
    impl.SYSTEM_COLUMN_INDEX_NAME: DEFAULT_INDEX_NAME,
    impl.SYSTEM_COLUMN_INDEX_VALUE_STRING: DEFAULT_INDEX_VALUE_STRING,
    impl.SYSTEM_COLUMN_INDEX_VALUE_NUMBER: DEFAULT_INDEX_VALUE_NUMBER,
    impl.SYSTEM_COLUMN_INDEX_VALUE_BLOB: DEFAULT_INDEX_VALUE_BLOB,
}


INDEX_ATTRS_QUOTED = {
    impl.SYSTEM_COLUMN_INDEX_NAME: DEFAULT_INDEX_NAME_QUOTED,
    impl.SYSTEM_COLUMN_INDEX_VALUE_STRING: DEFAULT_INDEX_VALUE_STRING_QUOTED,
    impl.SYSTEM_COLUMN_INDEX_VALUE_NUMBER: DEFAULT_INDEX_VALUE_NUMBER_QUOTED,
    impl.SYSTEM_COLUMN_INDEX_VALUE_BLOB: DEFAULT_INDEX_VALUE_BLOB_QUOTED,
}


DEFAULT_VALUE_FOR_TYPE_QUOTED = {
    'S': "''",
    'N': "0",
    'B': "0x",
    'SS': "{}",
    'NS': "{}",
    'BS': "{}"
}


INDEX_ATTR_FOR_TYPE = {
    'S': impl.SYSTEM_COLUMN_INDEX_VALUE_STRING,
    'N': impl.SYSTEM_COLUMN_INDEX_VALUE_NUMBER,
    'B': impl.SYSTEM_COLUMN_INDEX_VALUE_BLOB
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


def _json_to_attribute_map(json):
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
    CREATE KEYSPACE "{}"
    WITH REPLICATION = {{
        'class' : 'SimpleStrategy',
        'replication_factor' : 1 }};
    """


def init_test_env(tenant, table):
    try:
        STORAGE.session.execute(CREATE_SYSTEM_KEYSPACE)
    except Exception as e:
        print str(e)

    try:
        STORAGE.session.execute(CREATE_SYSTEM_TABLE)
    except Exception as e:
        print str(e)

    try:
        STORAGE.session.execute(
            CREATE_USER_KEYSPACE.format(USER_PREFIX + tenant))
    except Exception as e:
        print str(e)

    attrs = {
        'file_sha2': models.ATTRIBUTE_TYPE_STRING,
        'file_md5': models.ATTRIBUTE_TYPE_NUMBER
    }

    index_def_map = {'file_md5_index': models.IndexDefinition('file_md5')}

    schema = models.TableSchema(attrs, ['file_sha2'], index_def_map)

    try:
        STORAGE.create_table(Ctx(tenant), table, schema)
    except exception.TableAlreadyExistsException:
        STORAGE.session.execute('TRUNCATE {}.{}'.format(
            impl.USER_PREFIX + tenant,
            impl.USER_PREFIX + table))


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


def _make_key_conditions(table_info, attr_map):
    key_conditions = []

    for key in table_info.schema.key_attributes:
        if key in attr_map:
            typ, val = attr_map[key]
            key_conditions.append(
                USER_PREFIX + key + '=' +
                _encode_single_value_as_predefined_attr(typ, val)
            )

    return ' AND '.join(key_conditions)


def _make_index_conditions(index_name, string, number, blob):
    return "{} = '{}' AND {} = '{}' AND {} = {} AND {} = {}".format(
        impl.SYSTEM_COLUMN_INDEX_NAME, index_name,
        impl.SYSTEM_COLUMN_INDEX_VALUE_STRING, string,
        impl.SYSTEM_COLUMN_INDEX_VALUE_NUMBER, number,
        impl.SYSTEM_COLUMN_INDEX_VALUE_BLOB, blob)


def _make_default_index_conditions():
    return _make_index_conditions(
        DEFAULT_INDEX_NAME, DEFAULT_INDEX_VALUE_STRING,
        DEFAULT_INDEX_VALUE_NUMBER, DEFAULT_INDEX_VALUE_BLOB)


def _make_read_query(tenant, table_info, attr_map):
    query_builder = [
        'SELECT * FROM "{}"."{}" WHERE'.format(
            USER_PREFIX + tenant, table_info.internal_name)]

    query_builder.append(_make_key_conditions(table_info, attr_map))
    query_builder.append('AND')
    query_builder.append(_make_default_index_conditions())

    return " ".join(query_builder)


def _make_extra_values(attr_map):

    extra_exist_builder = []
    extra_data_builder = []
    extra_types_builder = []

    for attr_name, (typ, val) in attr_map.iteritems():
        quoted_name = "'{}'".format(attr_name)

        extra_exist_builder.append(quoted_name)
        extra_data_builder.append(
            quoted_name + ':' + _encode_dynamic_attr_value(typ, val))
        extra_types_builder.append(
            quoted_name + ':' + API_TO_QUOTED_CASSANDRA_TYPES[typ])

    return (
        '{' + ','.join(extra_exist_builder) + '}',
        '{' + ','.join(extra_data_builder) + '}',
        '{' + ','.join(extra_types_builder) + '}'
    )


def _make_main_insert_query(tenant, table_info,
                            attr_map, if_not_exists=False):
    query_builder = [
        'INSERT INTO "{}"."{}" ('.format(
            USER_PREFIX + tenant, table_info.internal_name)]

    fields = []
    values = []

    attr_map = attr_map.copy()

    for attr_name, attr_type in table_info.schema.attribute_type_map.iteritems():
        if attr_name in attr_map:
            fields.append(USER_PREFIX + attr_name)
            typ, val = attr_map[attr_name]
            # TODO ikhudoshyn: validate typ against attr_type
            values.append(_encode_predefined_attr_value(typ, val))
            del attr_map[attr_name]

    extra_exist, extra_data, extra_types = _make_extra_values(attr_map)

    fields.append(impl.SYSTEM_COLUMN_ATTR_EXIST)
    fields.append(impl.SYSTEM_COLUMN_EXTRA_ATTR_DATA)
    fields.append(impl.SYSTEM_COLUMN_EXTRA_ATTR_TYPES)

    values.append(extra_exist)
    values.append(extra_data)
    values.append(extra_types)

    if table_info.schema.index_def_map:
        fields.append(impl.SYSTEM_COLUMN_INDEX_NAME)
        fields.append(impl.SYSTEM_COLUMN_INDEX_VALUE_STRING)
        fields.append(impl.SYSTEM_COLUMN_INDEX_VALUE_NUMBER)
        fields.append(impl.SYSTEM_COLUMN_INDEX_VALUE_BLOB)

        values.append(DEFAULT_INDEX_NAME_QUOTED)
        values.append(DEFAULT_INDEX_VALUE_STRING_QUOTED)
        values.append(DEFAULT_INDEX_VALUE_NUMBER_QUOTED)
        values.append(DEFAULT_INDEX_VALUE_BLOB_QUOTED)

    query_builder.append(', '.join(fields))
    query_builder.append(') VALUES (')
    query_builder.append(', '.join(values))
    query_builder.append(')')

    if if_not_exists:
        query_builder.append(' IF NOT EXISTS')

    return ''.join(query_builder)


def _make_index_update_query(tenant, table_info, attr_map,
                             index_name, indexed_attr):

    if not indexed_attr in attr_map:
        return None

    attr_defs = table_info.schema.attribute_type_map.copy()
    key_attrs = table_info.schema.key_attributes[:]
    attr_map = attr_map.copy()

    hash_name = key_attrs.pop(0)

    query_builder = [
        'UPDATE "{}"."{}"'.format(USER_PREFIX + tenant, table_info.internal_name)]

    index_type, index_value = attr_map[indexed_attr]
    index_value_encoded = _encode_single_value_as_predefined_attr(
        index_type, index_value)

    where_attrs = INDEX_ATTRS_QUOTED.copy()
    where_attrs[impl.SYSTEM_COLUMN_INDEX_NAME] = "'" + index_name + "'"
    where_attrs[INDEX_ATTR_FOR_TYPE[index_type]] = index_value_encoded

    typ, val = attr_map[hash_name]
    where_attrs[USER_PREFIX + hash_name] = _encode_predefined_attr_value(
        typ, val)

    del attr_defs[hash_name]
    del attr_map[hash_name]

    for range_name in key_attrs:
        typ = attr_defs[range_name]
        where_attrs[USER_PREFIX + range_name] = DEFAULT_VALUE_FOR_TYPE_QUOTED[
            typ]

        del attr_defs[range_name]
        try:
            del attr_map[range_name]
        except KeyError:
            pass

    set_attrs = {}

    for attr, predef_typ in attr_defs.iteritems():
        if attr in attr_map:
            typ, val = attr_map[attr]

            # TODO ikhudoshyn: validate types
            # assert typ == predef_typ, 'Bang'

            set_attrs[USER_PREFIX + attr] = _encode_predefined_attr_value(
                typ, val)
            del attr_map[attr]

    extra_exist, extra_data, extra_types = _make_extra_values(attr_map)

    set_attrs[impl.SYSTEM_COLUMN_ATTR_EXIST] = extra_exist
    set_attrs[impl.SYSTEM_COLUMN_EXTRA_ATTR_DATA] = extra_data
    set_attrs[impl.SYSTEM_COLUMN_EXTRA_ATTR_TYPES] = extra_types

    query_builder.append('SET')

    set_builder = [
        '{} = {}'. format(nam, val)
        for nam, val in set_attrs.iteritems()
    ]

    query_builder.append(','.join(set_builder))

    query_builder.append('WHERE')

    where_builder = [
        '{} = {}'.format(nam, val)
        for nam, val in where_attrs.iteritems()
    ]

    query_builder.append(' AND '.join(where_builder))

    return ' '.join(query_builder)


def _make_insert_query(tenant, table_info, attr_map):
    query_builder = []

    indexed = table_info.schema.index_def_map

    if indexed:
        query_builder.append('BEGIN BATCH')

    query_builder.append(
        _make_main_insert_query(tenant, table_info, attr_map, indexed) + ';')

    if indexed:
        for index_name, index_def in indexed.iteritems():
            index_update_query = _make_index_update_query(
                tenant, table_info, attr_map, index_name,
                index_def.attribute_to_index)

            if index_update_query:
                query_builder.append(
                    index_update_query + ';')

        query_builder.append('APPLY BATCH')

    return ' '.join(query_builder)


def _make_insert_delete_query(tenant, table_info, attr_map, old_attrs):
    return (
        "BEGIN BATCH "
        "UPDATE {}.{} "
        "SET data = '{}', indexed = '{}' "
        "WHERE "
        "id = '{}' AND range = '' "
        "IF indexed = '{}'"
        "DELETE FROM {}.{} "
        "WHERE "
        "id = '{}' AND range = '{}'; "
        "UPDATE {}.{} "
        "SET data = '{}', indexed = '{}' "
        "WHERE "
        "id = '{}' AND range = '{}'; "
        "APPLY BATCH"
    ).format(TENANT, TABLE_NAME, data, indexed, id, old_indexed,
             TENANT, TABLE_NAME, id, old_indexed,
             TENANT, TABLE_NAME, data, indexed, id, indexed)


def _make_insert_update_query(tenant, table_info, attr_map):
    return (
        "BEGIN BATCH "
        "UPDATE {}.{} "
        "SET data = '{}' "
        "WHERE "
        "id = '{}' AND range = '' "
        "IF indexed = '{}'"
        "UPDATE {}.{} "
        "SET data = '{}' "
        "WHERE "
        "id = '{}' AND range = '{}'; "
        "APPLY BATCH"
    ).format(TENANT, TABLE_NAME, data, id, indexed,
             TENANT, TABLE_NAME, data, id, indexed)

queue_size = 1000
futures = Queue.Queue(maxsize=queue_size + 1)


def _is_applied(result):
    if not result:
        return True
    try:
        return result[0]['[applied]']
    except Exception:
        return False


def _parse_index_values(table_info, values):
    return {}


def _are_attrs_changed(old_attrs, new_attrs):
    return True


def _cb_read(result, futures, count, read_query, insert_query):
    LOG.debug("Read table {} : result {}".format(
        read_query, result))
    if not result:
        LOG.debug("Inserting new item {}".format(
            insert_query))
        query = insert_query
    else:
        old_attrs = _parse_index_values(table_info, result)

        if _are_attrs_changed(old_attrs, attr_map):
            query = _make_insert_delete_query(tenant, table_info, attr_map, old_attrs)
            LOG.debug("Inserting new item {} into '{}.{}', deleting {}: {}".format(
                attr_map, tenant, table_name, old_attrs, query))
        else:
            query = _make_insert_update_query(tenant, table_info, attr_map)
            LOG.debug("Inserting new item {} into '{}.{}', updating existing: {}".format(
                attr_map, tenant, table_name, query))

    future = STORAGE.session.execute_async(query)
    future.add_callback(_cb_insert, futures, count, read_query, insert_query)
    futures.put_nowait(future)
    count[0] += 1


def _cb_insert(result, futures, count, read_query, insert_query):
    LOG.debug("Insert {} result {}".format(
        insert_query, result))
    if not _is_applied(result):
        _read_table(futures, count, read_query, insert_query)


def put_item_async(futures, count, tenant, table_info, attr_map):
    insert_query = _make_insert_query(tenant, table_info, attr_map)

    if table_info.schema.index_def_map:
        read_query = _make_read_query(tenant, table_info, attr_map)
        _read_table(futures, count, read_query, insert_query)
    else:
        _insert(futures, count, insert_query)


def _read_table(futures, count, read_query, insert_query):
    LOG.debug("Reading table:{}".format(read_query))
    future = STORAGE.session.execute_async(read_query)
    future.add_callback(_cb_read, futures, count, read_query, insert_query)
    futures.put_nowait(future)
    count[0] += 1


def _insert(futures, count, insert_query):
    LOG.debug("Inserting item:{} ".format(insert_query))
    future = STORAGE.session.execute_async(insert_query)
    futures.put_nowait(future)
    count[0] += 1


def app_factory(global_conf, **local_conf):
    if not common.is_global_env_ready():
        options = dict(global_conf.items() + local_conf.items())
        oslo_config_args = options.get("oslo_config_args")
        s = string.Template(oslo_config_args)
        oslo_config_args = shlex.split(s.substitute(**options))
        common.setup_global_env(
            program=options.get("program", "magnetodb-api"),
            args=oslo_config_args)

        from magnetodb import storage
        STORAGE = storage.__STORAGE_IMPL
        global STORAGE

    return bulk_load_app


def bulk_load_app(environ, start_response):
    queue_size = 1000
    max_count = 100
    futures = Queue.Queue(maxsize=queue_size + 1)
    count = [0, 0]

    path = environ['PATH_INFO']

    LOG.debug('Request received, {}'.format(path))

    if not re.match("^/v1/\w+/data/tables/\w+/bulk_load$", path):
        start_response('404 Not found', [('Content-Type', 'text/html')])
        yield 'Incorrect url. Please check it and try again\n'
        return

    url_comp = path.split('/')
    tenant_name = url_comp[2]
    table_name = url_comp[5]

    LOG.debug('Tenant: {}, table name: {}'.format(tenant_name, table_name))

    init_test_env(tenant_name, table_name)

    context = Ctx(tenant_name)

    table_info = STORAGE._get_table_info(context, table_name)

    try:
        stream = environ['wsgi.input']
        for chunk in stream:
            data = json.loads(chunk)

            if count[0] % 1000 == 0:
                print count[0]

            while count[0] - count[1] >= max_count:
                try:
                    old_future = futures.get_nowait()
                    count[1] += 1
                    r =old_future.result()
                    LOG.debug('Future result:' + str(r))
                except Exception as e:
                    LOG.error(str(e))

            try:
                attr_map = _json_to_attribute_map(data)
                put_item_async(
                    futures, count, tenant_name, table_info, attr_map)
            except Exception as e:
                    LOG.error(str(e))

    except Exception as e:
                    LOG.error(str(e))

    while not futures.empty():
        f = futures.get_nowait()
        r = f.result()
        LOG.debug('Future result:' + str(r))

    start_response('200 OK', [('Content-Type', 'text/html')])
    yield 'Done\n'
