import collections
from magnetodb.storage import models

SYSTEM_KEYSPACE = 'magnetodb'

SYSTEM_COLUMN_INDEX_NAME = 'index_name'
SYSTEM_COLUMN_INDEX_VALUE_STRING = 'index_value_string'
SYSTEM_COLUMN_INDEX_VALUE_NUMBER = 'index_value_number'
SYSTEM_COLUMN_INDEX_VALUE_BLOB = 'index_value_blob'

SYSTEM_COLUMN_EXTRA_ATTR_DATA = 'extra_attr_data'
SYSTEM_COLUMN_EXTRA_ATTR_TYPES = 'extra_attr_types'
SYSTEM_COLUMN_ATTR_EXIST = 'attr_exist'

USER_PREFIX = 'user_'

class TableInfo(object):
    SYSTEM_TABLE_TABLE_INFO = SYSTEM_KEYSPACE + '.table_info'

    __field_list = ("schema", "internal_name", "status")

    def __init__(self, storage_driver, tenant, name, schema=None,
                 status=None, internal_name=None):
        self.__storage_driver = storage_driver
        self.__tenant = tenant
        self.__name = name

        self.schema = schema
        self.internal_name = internal_name
        self.status = status

    @property
    def tenant(self):
        return self.__tenant

    @property
    def name(self):
        return self.__name

    @classmethod
    def load(cls, storage_driver, tenant, table_name):
        table_info = TableInfo(storage_driver, tenant, table_name)
        return table_info if table_info.refresh() else None

    @classmethod
    def load_tenant_table_names(cls, storage_driver, tenant,
                                exclusive_start_table_name=None, limit=None):
        query_builder = collections.deque()
        query_builder.append("SELECT name")
        query_builder.append(" FROM ")
        query_builder.append(cls.SYSTEM_TABLE_TABLE_INFO)
        query_builder.append(" WHERE tenant='")
        query_builder.append(tenant)
        query_builder.append("'")

        if exclusive_start_table_name:
            query_builder.append(" AND name > '")
            query_builder.append(exclusive_start_table_name)
            query_builder.append("'")

        if limit:
            query_builder.append(" LIMIT ")
            query_builder.append(str(limit))

        tables = storage_driver._execute_query("".join(query_builder),
                                               consistent=True)

        return [row['name'] for row in tables]

    def refresh(self, *field_list):
        if not field_list:
            field_list = self.__field_list

        query_builder = collections.deque()
        query_builder.append("SELECT ")
        for field in field_list:
            query_builder.append('"')
            query_builder.append(field)
            query_builder.append('"')
            query_builder.append(",")
        query_builder.pop()

        query_builder.append(" FROM ")
        query_builder.append(self.SYSTEM_TABLE_TABLE_INFO)
        query_builder.append(" WHERE tenant='")
        query_builder.append(self.tenant)
        query_builder.append("' AND name='")
        query_builder.append(self.name)
        query_builder.append("'")

        result = self.__storage_driver._execute_query(
            "".join(query_builder), consistent=True
        )

        if result:
            for name, value in result[0].iteritems():
                if name == "schema":
                    value = models.ModelBase.from_json(value)
                setattr(self, name, value)
            return True
        else:
            return False

    def update(self, *field_list):
        if not field_list:
            field_list = self.__field_list

        query_builder = collections.deque()
        query_builder.append("UPDATE ")
        query_builder.append(self.SYSTEM_TABLE_TABLE_INFO)
        query_builder.append(" SET ")

        for field in field_list:
            query_builder.append('"')
            query_builder.append(field)
            query_builder.append('"=\'')
            query_builder.append(getattr(self, field))
            query_builder.append("'")
            query_builder.append(",")
        query_builder.pop()

        query_builder.append(" WHERE tenant='")
        query_builder.append(self.tenant)
        query_builder.append("' AND name='")
        query_builder.append(self.name)
        query_builder.append("' IF exists=1")

        result = self.__storage_driver._execute_query(
            "".join(query_builder), consistent=True
        )

        return result[0]['[applied]']

    def save(self):
        query_builder = collections.deque()
        query_builder.append("INSERT INTO ")
        query_builder.append(self.SYSTEM_TABLE_TABLE_INFO)
        query_builder.append(
            '(exists, tenant, name, "schema", status, internal_name)'
        )
        query_builder.append("VALUES(1,'")
        query_builder.append(self.tenant)
        query_builder.append("','")
        query_builder.append(self.name)
        query_builder.append("'")

        if self.schema:
            query_builder.append(",'")
            query_builder.append(self.schema.to_json())
            query_builder.append("'")
        else:
            query_builder.append(",null")

        if self.status:
            query_builder.append(",'")
            query_builder.append(self.status)
            query_builder.append("'")
        else:
            query_builder.append(",null")

        if self.internal_name:
            query_builder.append(",'")
            query_builder.append(self.internal_name)
            query_builder.append("'")
        else:
            query_builder.append(",null")

        query_builder.append(") IF NOT EXISTS")

        result = self.__storage_driver._execute_query(
            "".join(query_builder), consistent=True
        )

        return result[0]['[applied]']

    def delete(self):
        query_builder = collections.deque()
        query_builder.append("DELETE FROM ")
        query_builder.append(self.SYSTEM_TABLE_TABLE_INFO)
        query_builder.append(" WHERE tenant='")
        query_builder.append(self.tenant)
        query_builder.append("' AND name='")
        query_builder.append(self.name)
        query_builder.append("'")
        self.__storage_driver._execute_query("".join(query_builder),
                                             consistent=True)
