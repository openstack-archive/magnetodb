# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2014 OpenStack Foundation
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

from tempest.api.keyvalue.base import MagnetoDBTestCase
from tempest.common.utils.data_utils import rand_name
from tempest.common.utils.data_utils import rand_uuid
from tempest.openstack.common import log as logging

LOG = logging.getLogger(__name__)


class MagnetoDBItemsTest(MagnetoDBTestCase):

    def test_item_put_get_delete(self):

        tname = rand_name().replace('-', '')
        self.client.create_table(self.attrs,
                                 tname,
                                 self.schema,
                                 self.throughput,
                                 self.lsi,
                                 self.gsi)
        self.assertTrue(self.wait_for_table_active(tname))
        self.addResourceCleanUp(self.client.delete_table, tname)

        item = self.build_spam_item('a@mail.com', '2013-12-07T16:00:00.000001',
                                    rand_uuid(), 'b@mail.com', 'c@mail.com')
        resp = self.client.put_item(tname, item)
        self.assertDictEqual(resp, {})

        key = {'user_id': item['user_id'],
               'date_message_id': item['date_message_id']}

        resp = self.client.get_item(tname, key)
        self.assertDictEqual(item, resp['Item'])

        ## TODO(yyekovenko) ###########################################
        LOG.debug("UpdateItem action not implemented yet.")

        ## update item
        #attribute_updates = {
        #    'from_header': {
        #        'Value': {'S': 'updated'},
        #        'Action': 'PUT'}
        #}
        #resp = self.client.update_item(tname, key, attribute_updates)
        #self.assertEqual('?', '?')
        #resp = self.client.get_item(tname, key)
        #self.assertEqual(resp['Item']['from_header'], 'updated')
        ##############################################################

        resp = self.client.delete_item(tname, key)
        self.assertDictEqual(resp, {})
        self.assertDictEqual(self.client.get_item(tname, key), {})
