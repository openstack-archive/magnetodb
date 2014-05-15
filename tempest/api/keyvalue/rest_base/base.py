# Copyright 2012 OpenStack Foundation
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

import tempest.clients
import tempest.config
import tempest.test
from tempest.common.utils import data_utils
from tempest.openstack.common import log as logging


LOG = logging.getLogger(__name__)


class MagnetoDBTestCase(tempest.test.BaseTestCase):

    @classmethod
    def setUpClass(cls):
        super(MagnetoDBTestCase, cls).setUpClass()
        cls.os = tempest.clients.Manager()
        cls.client = cls.os.magnetodb_client
        cls._sequence = -1
        cls._resource_trash_bin = {}

        # SMOKE TABLE: THREADS
        cls.hashkey = 'forum'
        cls.rangekey = 'subject'

        cls.smoke_attrs = [
            {'attribute_name': cls.hashkey, 'attribute_type': 'S'},
            {'attribute_name': cls.rangekey, 'attribute_type': 'S'}
        ]
        cls.index_attrs = [
            {'attribute_name': 'last_posted_by', 'attribute_type': 'S'},
            # {'attribute_name': 'message', 'attribute_type': 'S'},
            # {'attribute_name': 'replies', 'attribute_type': 'N'}
        ]
        cls.smoke_schema = [
            {'attribute_name': cls.hashkey, 'key_type': 'HASH'},
            {'attribute_name': cls.rangekey, 'key_type': 'RANGE'}
        ]
        cls.one_attr = [
            {'attribute_name': cls.hashkey, 'attribute_type': 'S'}
        ]
        cls.schema_hash_only = [
            {'attribute_name': cls.hashkey, 'key_type': 'HASH'}
        ]
        cls.smoke_lsi = [
            {
                'index_name': 'last_posted_by_index',
                'key_schema': [
                    {'attribute_name': cls.hashkey, 'key_type': 'HASH'},
                    {'attribute_name': 'last_posted_by', 'key_type': 'RANGE'}
                ],
                'projection': {'projection_type': 'ALL'}
            }
        ]

    @classmethod
    def tearDownClass(cls):
        """Calls the callables added by addResourceCleanUp,
        when you overwire this function dont't forget to call this too.
        """
        fail_count = 0
        trash_keys = sorted(cls._resource_trash_bin, reverse=True)
        for key in trash_keys:
            (function, pos_args, kw_args) = cls._resource_trash_bin[key]
            try:
                LOG.debug("Cleaning up: %s" %
                          friendly_function_call_str(function, *pos_args,
                                                     **kw_args))
                function(*pos_args, **kw_args)
            except BaseException as exc:
                fail_count += 1
                LOG.exception(exc)
            finally:
                del cls._resource_trash_bin[key]
        super(MagnetoDBTestCase, cls).tearDownClass()
        if fail_count:
            LOG.error('%s cleanUp operation failed' % fail_count)

    @classmethod
    def addResourceCleanUp(cls, function, *args, **kwargs):
        """Adds CleanUp callable, used by tearDownClass.
        Recommended to a use (deep)copy on the mutable args.
        """
        cls._sequence = cls._sequence + 1
        cls._resource_trash_bin[cls._sequence] = (function, args, kwargs)
        return cls._sequence

    @classmethod
    def cancelResourceCleanUp(cls, key):
        """Cancel Clean up request."""
        del cls._resource_trash_bin[key]

    @classmethod
    def wait_for_table_active(cls, table_name, timeout=120, interval=3):
        def check():
            headers, body = cls.client.describe_table(table_name)
            if "table" in body and "table_status" in body["table"]:
                return body["table"]["table_status"] == "ACTIVE"

        return tempest.test.call_until_true(check, timeout, interval)

    def wait_for_table_deleted(self, table_name, timeout=120, interval=3):
        def check():
            return table_name not in self.client.list_tables()['table_names']

        return tempest.test.call_until_true(check, timeout, interval)

    @staticmethod
    def build_smoke_item(forum, subject, message='message_text',
                         last_posted_by='John', replies='1'):
        return {
            "forum": {"S": forum},
            "subject": {"S": subject},
            "message": {"S": message},
            "last_posted_by": {"S": last_posted_by},
            "replies": {"N": replies},
        }

    @classmethod
    def build_x_item(cls, attr_type, forum, subject, *args):
        item_dict = {
            cls.hashkey: {attr_type: forum},
            cls.rangekey: {attr_type: subject},
        }
        for n, t, v in args:
            item_dict[n] = {t: v}
        return item_dict

    @classmethod
    def build_x_attrs(cls, attr_type):
        return [
            {'attribute_name': cls.hashkey, 'attribute_type': attr_type},
            {'attribute_name': cls.rangekey, 'attribute_type': attr_type}
        ]

    def put_smoke_item(self, table_name,
                       forum, subject, message='message_text',
                       last_posted_by='John', replies='1'):

        item = self.build_smoke_item(forum, subject, message,
                                     last_posted_by, replies)
        self.client.put_item(table_name, item)
        return item

    def populate_smoke_table(self, table_name, keycount, count_per_key):
        """
        Put [keycont*count_per_key] autogenerated items to the table.

        In result, [keycount] unique hash key values
        and [count_per_key] items for each has key value are generated.

        For example, to generate some number of items for the only hash key,
        set keycount=1 and count_per_key=needed_number_of_items.
        """
        new_items = []
        for _ in range(keycount):
            forum = 'forum%s' % data_utils.rand_int_id()
            for i in range(count_per_key):
                item = self.put_smoke_item(
                    table_name, forum=forum, subject='subject%s' % i,
                    message=data_utils.rand_name(),
                    last_posted_by=data_utils.rand_uuid(),
                    replies=str(data_utils.rand_int_id())
                )
                new_items.append(item)
        return new_items

    def _create_test_table(self, attr_def, tname, *args, **kwargs):
        cleanup = kwargs.pop('cleanup', True)
        wait_for_active = kwargs.pop('wait_for_active', False)
        headers, body = self.client.create_table(attr_def,
                                                 tname,
                                                 *args,
                                                 **kwargs)
        if cleanup:
            self.addResourceCleanUp(self.client.delete_table, tname)
        if wait_for_active:
            self.wait_for_table_active(tname)

        return headers, body


def friendly_function_name_simple(call_able):
    name = ""
    if hasattr(call_able, "im_class"):
        name += call_able.im_class.__name__ + "."
    name += call_able.__name__
    return name


def friendly_function_call_str(call_able, *args, **kwargs):
    string = friendly_function_name_simple(call_able)
    string += "(" + ", ".join(map(str, args))
    if len(kwargs):
        if len(args):
            string += ", "
    string += ", ".join("=".join(map(str, (key, value)))
                        for (key, value) in kwargs.items())
    return string + ")"
