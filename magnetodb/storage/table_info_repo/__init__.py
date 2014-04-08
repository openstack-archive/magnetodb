# Copyright 2013 Mirantis Inc.
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


class TableInfo(object):

    def __init__(self, name, schema=None, status=None):
        self.name = name
        self.schema = schema
        self.status = status


class TableInfoRepository(object):

    def get(self, context, table_name):
        raise NotImplementedError()

    def get_tenant_table_names(self, context, exclusive_start_table_name=None,
                               limit=None):
        raise NotImplementedError()

    def update(self, table_info, field_list=None):
        raise NotImplementedError()

    def refresh(self, table_info, field_list=None):
        raise NotImplementedError()

    def save(self, context, table_info):
        raise NotImplementedError()

    def delete(self, context, table_name):
        raise NotImplementedError()
