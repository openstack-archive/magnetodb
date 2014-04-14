#!/usr/bin/env python

import base64
import json
import sys

import requests


def read_table_structure(filename):
    structure = []
    with open(filename, 'r') as f:
        for line in f:
            field, typ = line.strip().split()
            structure.append((field, typ))

    return structure


def decode_value(typ, value):
    if typ == 'S':
        return value
    elif typ == 'B':
        return base64.encodestring(value).strip()
    elif typ == 'N':
        return value if value else '0'
    elif typ in ('NS', 'SS', 'BS'):
        vals = value[1:-1].split(',')

        single_type = typ[0]

        if single_type == 'N':
            return [v.strip() if v.strip() else '0'
                    for v in vals]
        elif single_type == 'B':
            return [base64.encodestring(v.strip()).strip()
                    for v in vals]
        else:
            return [v.strip()
                    for v in vals]

        assert False


def make_json(structure, item):
    tokens = item.strip().split('\x01')

    assert len(structure) == len(tokens)

    res = {
        field: {typ: decode_value(typ, value.strip())}
        for (field, typ), value
        in zip(structure, tokens)
        if value.strip()
    }

    first_name, _ = structure[0]

    res['id'] = res[first_name]

    return res


def get_items(table_def_filename, data_filename):
    table_str = read_table_structure(table_def_filename)

    with open(data_filename, 'r') as f:
        for line in f:
            yield json.dumps(make_json(table_str, line)) + '\n'


if __name__ == '__main__':

    if len(sys.argv) != 3:
        print 'python {} <table_def.file> <data.file>'.format(sys.argv[0])

    table_def_filename, data_filename = sys.argv[1:]

    r = requests.post('http://localhost:9999/v1/default_tenant/data/tables/bigdata/bulk_load',
                      data=get_items(table_def_filename, data_filename),
                      headers={'Content-Type': 'application/json'})

    print r.content
