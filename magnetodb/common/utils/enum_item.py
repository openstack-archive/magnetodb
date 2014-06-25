# Copyright 2014 Mirantis Inc.
#  All Rights Reserved.
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
#
#
from magnetodb.openstack.common.gettextutils import _


class BadEnumItemIdException(Exception):
    pass


class EnumItem(object):
    _allowed_item_ids = []

    def __eq__(self, other):
        if isinstance(other, basestring):
            return self.id == other
        return super(EnumItem, self).__eq__(other)

    def __init__(self, item_id):
        if item_id not in self._allowed_item_ids:
            raise BadEnumItemIdException(
                _('%s not in valid item id') % item_id)
        self.__id = item_id

    @property
    def id(self):
        return self.__id