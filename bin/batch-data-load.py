#!/usr/bin/env python
import binascii
import ujson as json
import sys
from magnetodb.api.openstack.v1 import parser

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
        return binascii.b2a_base64(value)
    elif typ == 'N':
        return value if value else '0'
    elif typ in ('NS', 'SS', 'BS'):
        vals = value[1:-1].split(',')

        single_type = typ[0]

        if single_type == 'N':
            return [v.strip() if v.strip() else '0'
                    for v in vals]
        elif single_type == 'B':
            return [binascii.b2a_base64(v.strip()).strip()
                    for v in vals]
        else:
            return [v.strip()
                    for v in vals]

        assert False


def make_json(structure, item):
    tokens = item.strip().split('\x01')

    assert len(structure) == len(tokens)

    return {
        field: {typ: decode_value(typ, value.strip())}
        for (field, typ), value
        in zip(structure, tokens)
        if value.strip()
    }


def execute_batch(tenant, table_name, request_list):
    batch = {
        parser.Props.REQUEST_ITEMS: {
            table_name: request_list
        }
    }
    res = requests.post(
        'http://localhost:8480/v1/{}/data/batch_write_item'.format(tenant),
        data=json.dumps(batch),
        headers={'Content-Type': 'application/json'}
    )
    return res.content


if __name__ == '__main__':
    tenant = "default_tenant"
    table_name = "bigdata"
    if len(sys.argv) != 3:
        print 'python {} <table_def.file> <data.file>'.format(sys.argv[0])

    table_def_filename, data_filename = sys.argv[1:]

    table_str = read_table_structure(table_def_filename)

    with open(data_filename, 'r') as f:
        requests_per_batch = 25
        count = 0

        request_list = []

        for line in f:
            request_list.append(
                {
                    parser.Props.REQUEST_PUT: {
                        parser.Props.ITEM: make_json(table_str, line)
                    }
                }
            )
            count += 1

            if len(request_list) >= requests_per_batch:
                res = execute_batch(tenant, table_name, request_list)
                request_list = []
                print res + "\n"
                print "{} items processed".format(count) + "\n"

    if len(request_list) > 0:
        res = execute_batch(tenant, table_name, request_list)
        request_list = []
        print res + "\n"
        print "{} items processed".format(count) + "\n"
