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

from magnetodb.storage import models


class TableInfo(object):
    def __init__(self, name, id, schema, status, internal_name=None,
                 creation_date_time=None, last_update_date_time=None):
        self.__name = name
        self.id = id
        self.schema = schema
        self.status = status
        self.internal_name = internal_name
        self.creation_date_time = creation_date_time
        self.last_update_date_time = last_update_date_time

    @property
    def in_use(self):
        return (
            self.status == models.TableMeta.TABLE_STATUS_CREATING or
            self.status == models.TableMeta.TABLE_STATUS_DELETING
        )

    @property
    def name(self):
        return self.__name

    def __hash__(self):
        return hash(self.__name)


class TableInfoRepository(object):

    def get(self, tenant, table_name, fields_to_refresh):
        raise NotImplementedError()

    def list_tables(self, tenant, exclusive_start_table_name=None, limit=None):
        raise NotImplementedError()

    def list_all_tables(self, last_evaluated_tenant=None,
                        last_evaluated_table=None, limit=None):
        raise NotImplementedError()

    def update(self, tenant, table_info, field_list=None):
        raise NotImplementedError()

    def save(self, tenant, table_info):
        raise NotImplementedError()

    def delete(self, tenant, table_name):
        raise NotImplementedError()
