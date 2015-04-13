# Copyright 2014 Symantec Corporation
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

import base64
import copy

from tempest.api.keyvalue.rest_base import base
from tempest_lib.common.utils.data_utils import rand_name
from tempest_lib import exceptions


INDEX_NAME_N = "by_number"
INDEX_NAME_S = "by_str"
INDEX_NAME_B = "by_blob"

ATTRIBUTE_DEFINITIONS = [
    {
        "attribute_name": "hash_attr",
        "attribute_type": "S"
    },
    {
        "attribute_name": "range_attr",
        "attribute_type": "S"
    },
    {
        "attribute_name": "istr",
        "attribute_type": "S"
    },
    {
        "attribute_name": "inumber",
        "attribute_type": "N"
    },
    {
        "attribute_name": "iblob",
        "attribute_type": "B"
    }
]

KEY_SCHEMA = [
    {
        "attribute_name": "hash_attr",
        "key_type": "HASH"
    },
    {
        "attribute_name": "range_attr",
        "key_type": "RANGE"
    }
]

LSI_INDEXES = [
    {
        "index_name": INDEX_NAME_S,
        "key_schema": [
            {
                "attribute_name": "hash_attr",
                "key_type": "HASH"
            },
            {
                "attribute_name": "istr",
                "key_type": "RANGE"
            }
        ],
        "projection": {
            "projection_type": "ALL"
        }
    },
    {
        "index_name": INDEX_NAME_N,
        "key_schema": [
            {
                "attribute_name": "hash_attr",
                "key_type": "HASH"
            },
            {
                "attribute_name": "inumber",
                "key_type": "RANGE"
            }
        ],
        "projection": {
            "projection_type": "ALL"
        }
    },
    {
        "index_name": INDEX_NAME_B,
        "key_schema": [
            {
                "attribute_name": "hash_attr",
                "key_type": "HASH"
            },
            {
                "attribute_name": "iblob",
                "key_type": "RANGE"
            }
        ],
        "projection": {
            "projection_type": "ALL"
        }
    }
]

ITEM_PRIMARY_KEY = {
    "hash_attr": {"S": "1"},
    "range_attr": {"S": "1"}
}

ITEM_PRIMARY_KEY_ALT = {
    "hash_attr": {"S": "2"},
    "range_attr": {"S": "2"}
}

KEY_CONDITIONS = {
    "hash_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    },
    "range_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    }
}

KEY_CONDITIONS_INDEX = {
    "hash_attr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    },
    "inumber": {
        "attribute_value_list": [{"N": "1"}],
        "comparison_operator": "EQ"
    }
}

SCAN_FILTER = {
    "inumber": {
        "attribute_value_list": [{"N": "1"}],
        "comparison_operator": "EQ"
    },
    "istr": {
        "attribute_value_list": [{"S": "1"}],
        "comparison_operator": "EQ"
    }
}

ADDITIONAL_FIELDS = {
    "inumber": {"N": "1"},
    "istr": {"S": "1"},
    "iblob": {"B": base64.b64encode('\x01')},
    "inumberset": {"NS": ["1", "2", "3"]},
    "istringset": {"SS": ["1", "2", "3"]},
    "iblobset": {"BS": [base64.b64encode("\x01"), base64.b64encode("\x02"),
                        base64.b64encode("\x03")]},
}

ADDITIONAL_FIELDS_ALT = {
    "inumber": {"N": "2"},
    "istr": {"S": "2"},
    "iblob": {"B": base64.b64encode('\x02')},
    "inumberset": {"NS": ["4", "5", "6"]},
    "istringset": {"SS": ["4", "5", "6"]},
    "iblobset": {"BS": [base64.b64encode("\x04"), base64.b64encode("\x05"),
                        base64.b64encode("\x06")]},
}

ITEM = copy.copy(ITEM_PRIMARY_KEY)
ITEM.update(ADDITIONAL_FIELDS)

ITEM_ALT = copy.copy(ITEM_PRIMARY_KEY_ALT)
ITEM_ALT.update(ADDITIONAL_FIELDS_ALT)


ATTRIBUTES_UPDATE = {
    "inumber": {
        "action": "PUT",
        "value": {"N": "1"},
    }
}


def _local_update(item, attr_update):
    updated_item = copy.copy(item)
    updated_item.update({k: v['value'] for k, v in attr_update.iteritems()
                         if v['action'] == 'PUT'})
    return updated_item


class MagnetoDBTableOperationsTestCase(base.MagnetoDBTestCase):

    force_tenant_isolation = True

    def test_table_operations(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        url = '{url}/tables/{table}'.format(
            url=self.client.base_url, table=tname)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertNotIn(url, tables)

        not_found_msg = "'%s' does not exist" % tname
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.delete_table, tname)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.get_item, tname, ITEM_PRIMARY_KEY)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.query, tname,
                              key_conditions=KEY_CONDITIONS)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.query, tname,
                              index_name=INDEX_NAME_N,
                              key_conditions=KEY_CONDITIONS_INDEX)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.scan, tname,
                              scan_filter=SCAN_FILTER)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.put_item, tname, ITEM)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.update_item, tname, ITEM_PRIMARY_KEY,
                              ATTRIBUTES_UPDATE)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.delete_item, tname, ITEM_PRIMARY_KEY)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.describe_table, tname)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertNotIn(url, tables)

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)

        exc_message = 'Table %s already exists' % tname
        self._check_exception(exceptions.BadRequest, exc_message,
                              self.client.create_table,
                              ATTRIBUTE_DEFINITIONS,
                              tname,
                              KEY_SCHEMA,
                              LSI_INDEXES)

        self.assertTrue(self.wait_for_table_active(tname))

        self._check_exception(exceptions.BadRequest, exc_message,
                              self.client.create_table,
                              ATTRIBUTE_DEFINITIONS,
                              tname,
                              KEY_SCHEMA,
                              LSI_INDEXES)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertIn(url, tables)

        self.client.put_item(tname, ITEM)
        self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.client.query(tname, key_conditions=KEY_CONDITIONS_INDEX,
                          index_name=INDEX_NAME_N)
        self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.client.update_item(tname, ITEM_PRIMARY_KEY, ATTRIBUTES_UPDATE)
        self.client.delete_item(tname, ITEM_PRIMARY_KEY)
        self.client.describe_table(tname)
        self.client.delete_table(tname)

        self.assertTrue(self.wait_for_table_deleted(tname))

        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.client.delete_table, tname)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertNotIn(url, tables)

        # checking that data in the table is not accessible after table
        # deletion
        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname)
        self.client.put_item(tname, ITEM)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(1, body['count'])

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(0, body['count'])

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)


class MagnetoDBItemsOperationsTestCase(base.MagnetoDBTestCase):

    def test_items_non_indexed_table(self):
        tname = rand_name(self.table_prefix).replace('-', '')

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA)

        self.wait_for_table_active(tname)

        # retrive non-existing
        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({}, body)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(0, body['count'])
        headers, body = self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.assertEqual(0, body['count'])

        # put item
        self.client.put_item(tname, ITEM)
        self.client.put_item(tname, ITEM)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual(ITEM, body['item'])
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(1, len(body['items']))
        self.assertEqual(ITEM, body['items'][0])
        headers, body = self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.assertEqual(1, len(body['items']))
        self.assertEqual(ITEM, body['items'][0])

        # extend this test after fixing bug #1348336

        # update item
        self.client.update_item(tname, ITEM_PRIMARY_KEY, ATTRIBUTES_UPDATE)
        updated_item = _local_update(ITEM, ATTRIBUTES_UPDATE)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual(updated_item, body['item'])

        # delete item
        self.client.delete_item(tname, ITEM_PRIMARY_KEY)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({}, body)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(0, body['count'])
        headers, body = self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.assertEqual(0, body['count'])

        # check for no exception
        self.client.delete_item(tname, ITEM_PRIMARY_KEY)

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)

    def test_items_indexed_table(self):
        tname = rand_name(self.table_prefix).replace('-', '')

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)

        self.wait_for_table_active(tname)

        # retrive non-existing
        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({}, body)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(0, body['count'])
        headers, body = self.client.query(tname,
                                          key_conditions=KEY_CONDITIONS_INDEX,
                                          index_name=INDEX_NAME_N)
        self.assertEqual(0, body['count'])

        headers, body = self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.assertEqual(0, body['count'])

        # put item
        self.client.put_item(tname, ITEM)
        self.client.put_item(tname, ITEM)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual(ITEM, body['item'])
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(1, len(body['items']))
        self.assertEqual(ITEM, body['items'][0])
        headers, body = self.client.query(tname,
                                          key_conditions=KEY_CONDITIONS_INDEX,
                                          index_name=INDEX_NAME_N)
        self.assertEqual(1, len(body['items']))
        self.assertEqual(ITEM, body['items'][0])
        headers, body = self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.assertEqual(1, len(body['items']))
        self.assertEqual(ITEM, body['items'][0])

        # extend this put cases after fixing bug #1348336

        # update item
        self.client.update_item(tname, ITEM_PRIMARY_KEY, ATTRIBUTES_UPDATE)
        updated_item = _local_update(ITEM, ATTRIBUTES_UPDATE)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual(updated_item, body['item'])

        # delete item
        self.client.delete_item(tname, ITEM_PRIMARY_KEY)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({}, body)
        headers, body = self.client.query(tname, key_conditions=KEY_CONDITIONS)
        self.assertEqual(0, body['count'])
        headers, body = self.client.query(tname,
                                          key_conditions=KEY_CONDITIONS_INDEX,
                                          index_name=INDEX_NAME_N)
        self.assertEqual(0, body['count'])
        headers, body = self.client.scan(tname, scan_filter=SCAN_FILTER)
        self.assertEqual(0, body['count'])

        # check for no exception
        self.client.delete_item(tname, ITEM_PRIMARY_KEY)

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)

    def test_batch_operations(self):
        tables = []
        for i in range(0, 5):
            tname = rand_name(self.table_prefix).replace('-', '')
            headers, body = self.client.create_table(
                ATTRIBUTE_DEFINITIONS,
                tname,
                KEY_SCHEMA,
                LSI_INDEXES)
            self.wait_for_table_active(tname)
            tables.append(tname)

        request_items = {
            "request_items": {
                tname: [{"put_request": {"item": ITEM}}] for tname in tables
            }
        }
        headers, body = self.client.batch_write_item(request_items)
        self.assertEqual({}, body['unprocessed_items'])

        request_items = {
            "request_items": {
                tname: {"keys": [ITEM_PRIMARY_KEY]} for tname in tables
            }
        }
        headers, body = self.client.batch_get_item(request_items)
        self.assertEqual({}, body['unprocessed_keys'])
        for tname in tables:
            self.assertEqual(ITEM, body['responses'][tname][0])

        request_items = {
            "request_items": {
                tname: [
                    {"put_request": {"item": ITEM_ALT}},
                    {"delete_request": {"key": ITEM_PRIMARY_KEY}}
                ] for tname in tables
            }
        }
        headers, body = self.client.batch_write_item(request_items)
        self.assertEqual({}, body['unprocessed_items'])
        request_items = {
            "request_items": {
                tname: {"keys": [ITEM_PRIMARY_KEY_ALT]} for tname in tables
            }
        }
        headers, body = self.client.batch_get_item(request_items)
        self.assertEqual({}, body['unprocessed_keys'])
        for tname in tables:
            self.assertEqual(1, len(body['responses'][tname]))
            self.assertEqual(ITEM_ALT, body['responses'][tname][0])

        request_items = {
            "request_items": {
                tname: [
                    {"put_request": {"item": ITEM_PRIMARY_KEY_ALT}}
                ] for tname in tables
            }
        }
        headers, body = self.client.batch_write_item(request_items)
        self.assertEqual({}, body['unprocessed_items'])
        request_items = {
            "request_items": {
                tname: {"keys": [ITEM_PRIMARY_KEY_ALT]} for tname in tables
            }
        }
        headers, body = self.client.batch_get_item(request_items)
        self.assertEqual({}, body['unprocessed_keys'])
        for tname in tables:
            self.assertEqual(ITEM_PRIMARY_KEY_ALT, body['responses'][tname][0])

        for tname in tables:
            self.client.delete_table(tname)
            self.wait_for_table_deleted(tname)

    def test_bulk_operations(self):
        tname = rand_name(self.table_prefix).replace('-', '')

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)

        self.wait_for_table_active(tname)
        self.streaming_client.upload_items(tname, [ITEM, ITEM_ALT])
        # checking if data was uploaded
        request_items = {
            "request_items": {
                tname: {"keys": [ITEM_PRIMARY_KEY, ITEM_PRIMARY_KEY_ALT]}
            }
        }
        headers, body = self.client.batch_get_item(request_items)
        self.assertEqual({}, body['unprocessed_keys'])
        self.assertEqual(2, len(body['responses'][tname]))
        self.assertIn(ITEM, body['responses'][tname])
        self.assertIn(ITEM_ALT, body['responses'][tname])

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)


class MagnetoDBMultitenancyTableTestCase(base.MagnetoDBMultitenancyTestCase):

    force_tenant_isolation = True

    def test_table_operations(self):
        tname = rand_name(self.table_prefix).replace('-', '')
        url = '{url}/tables/{table}'.format(
            url=self.client.base_url, table=tname)
        url_alt = '{url}/tables/{table}'.format(
            url=self.alt_client.base_url, table=tname)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertNotIn(url, tables)
        resp, body = self.alt_client.list_tables()
        tables_alt = [table['href'] for table in body['tables']]
        self.assertNotIn(url_alt, tables_alt)

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        headers, body = self.alt_client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertIn(url, tables)
        resp, body = self.alt_client.list_tables()
        tables_alt = [table['href'] for table in body['tables']]
        self.assertIn(url_alt, tables_alt)

        self.wait_for_table_active(tname)
        self.wait_for_table_active(tname, alt=True)

        self.alt_client.delete_table(tname)
        self.wait_for_table_deleted(tname, alt=True)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertIn(url, tables)
        resp, body = self.alt_client.list_tables()
        tables_alt = [table['href'] for table in body['tables']]
        self.assertNotIn(url_alt, tables_alt)

        headers, body = self.alt_client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname, alt=True)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertIn(url, tables)
        resp, body = self.alt_client.list_tables()
        tables_alt = [table['href'] for table in body['tables']]
        self.assertIn(url_alt, tables_alt)

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertNotIn(url, tables)
        resp, body = self.alt_client.list_tables()
        tables_alt = [table['href'] for table in body['tables']]
        self.assertIn(url_alt, tables_alt)

        self.alt_client.delete_table(tname)
        self.wait_for_table_deleted(tname, alt=True)


class MagnetoDBMultitenancyItemsTestCase(base.MagnetoDBMultitenancyTestCase):

    force_tenant_isolation = True

    def test_item_operations(self):
        tname = rand_name(self.table_prefix).replace('-', '')

        resp, body = self.client.list_tables()
        tables = [table['href'] for table in body['tables']]
        self.assertNotIn(tname, tables)
        resp, body = self.alt_client.list_tables()
        tables_alt = [table['href'] for table in body['tables']]
        self.assertNotIn(tname, tables_alt)

        headers, body = self.client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname)

        self.client.put_item(tname, ITEM)

        not_found_msg = "'%s' does not exist" % tname
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.alt_client.get_item, tname,
                              ITEM_PRIMARY_KEY)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.alt_client.put_item, tname,
                              ITEM)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.alt_client.update_item, tname,
                              ITEM_PRIMARY_KEY, ATTRIBUTES_UPDATE)
        self._check_exception(exceptions.NotFound, not_found_msg,
                              self.alt_client.delete_item, tname,
                              ITEM_PRIMARY_KEY)

        headers, body = self.alt_client.create_table(
            ATTRIBUTE_DEFINITIONS,
            tname,
            KEY_SCHEMA,
            LSI_INDEXES)
        self.wait_for_table_active(tname, alt=True)

        headers, body = self.alt_client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({}, body)

        item = copy.copy(ITEM_PRIMARY_KEY)
        item.update(ADDITIONAL_FIELDS_ALT)
        self.alt_client.put_item(tname, item)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({'item': ITEM}, body)

        headers, body = self.alt_client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({'item': item}, body)

        self.alt_client.update_item(tname, ITEM_PRIMARY_KEY, ATTRIBUTES_UPDATE)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({'item': ITEM}, body)

        headers, body = self.alt_client.get_item(tname, ITEM_PRIMARY_KEY)
        updated_item = _local_update(item, ATTRIBUTES_UPDATE)
        self.assertEqual({'item': updated_item}, body)

        self.alt_client.delete_item(tname, ITEM_PRIMARY_KEY)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({'item': ITEM}, body)

        headers, body = self.alt_client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({}, body)

        self.alt_client.delete_table(tname)
        self.wait_for_table_deleted(tname, alt=True)

        headers, body = self.client.get_item(tname, ITEM_PRIMARY_KEY)
        self.assertEqual({'item': ITEM}, body)
        # uncomment when fixed
#        self._check_exception(exceptions.NotFound, not_found_msg,
#                              self.alt_client.get_item, tname,
#                              ITEM_PRIMARY_KEY)

        self.client.delete_table(tname)
        self.wait_for_table_deleted(tname)
