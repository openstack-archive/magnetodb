import pecan
from pecan import rest

from magnetodb import api

from magnetodb.api.openstack.v1.data import put_item
from magnetodb.api.openstack.v1.data import get_item
from magnetodb.api.openstack.v1.data import delete_item
from magnetodb.api.openstack.v1.data import update_item
from magnetodb.api.openstack.v1.data import scan
from magnetodb.api.openstack.v1.data import query
from magnetodb.api.openstack.v1.data import delete_table
from magnetodb.api.openstack.v1.data import create_table
from magnetodb.api.openstack.v1.data import list_tables
from magnetodb.api.openstack.v1.data import describe_table
from magnetodb.api.openstack.v1.data import batch_write_item
from magnetodb.api.openstack.v1.data import batch_get_item


class TablesController(rest.RestController):
    _custom_actions = {
        'put_item': ['POST'],
        'get_item': ['POST'],
        'delete_item': ['POST'],
        'update_item': ['POST'],
        'scan': ['POST'],
        'query': ['POST']
    }

    @pecan.expose("json")
    def put_item(self, project_id, table_name):
        if not table_name:
            pecan.abort(404)
        return put_item.put_item(pecan.request, project_id,
                                 table_name)

    @pecan.expose("json")
    def get_item(self, project_id, table_name=None):
        if not table_name:
            pecan.abort(404)
        return get_item.get_item(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def delete_item(self, project_id, table_name):
        if not table_name:
            pecan.abort(404)
        return delete_item.delete_item(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def update_item(self, project_id, table_name=None):
        if not table_name:
            pecan.abort(404)
        return update_item.update_item(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def scan(self, project_id, table_name=None):
        if not table_name:
            pecan.abort(404)
        return scan.scan(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def query(self, project_id, table_name=None):
        if not table_name:
            pecan.abort(404)
        return query.query(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def delete(self, project_id, table_name=None):
        if not table_name:
            pecan.abort(404)
        return delete_table.delete_table(pecan.request, project_id, table_name)

    @pecan.expose("json")
    def post(self, project_id):
        return create_table.create_table(pecan.request, project_id)

    @pecan.expose("json")
    def get_all(self, project_id):
        return list_tables.list_tables(pecan.request, project_id)

    @pecan.expose("json")
    def get_one(self, project_id, table_name):
        return describe_table.describe_table(pecan.request, project_id,
                                             table_name)


class MagnetoDBRootController(rest.RestController):

    """API"""
    _custom_actions = {
        'batch_write_item': ['POST'],
        'batch_get_item': ['POST']
    }

    @pecan.expose("json")
    def batch_write_item(self, project_id=None):
        if not project_id:
            pecan.abort(404)
        return batch_write_item.batch_write_item(
            pecan.request, project_id
        )

    @pecan.expose("json")
    def batch_get_item(self, project_id=None):
        if not project_id:
            raise pecan.abort(404)
        return batch_get_item.batch_get_item(pecan.request, project_id)

    @pecan.expose("json")
    def get_one(self, project_id):
        pecan.abort(404)

    tables = TablesController()


@api.with_global_env(default_program='magnetodb-api')
def app_factory(global_conf, **local_conf):
    return pecan.make_app(
        root="magnetodb.api.openstack.v1.data.MagnetoDBRootController",
        force_canonical=True,
        guess_content_type_from_ext=False
    )