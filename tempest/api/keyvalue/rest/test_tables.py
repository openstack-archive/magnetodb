from tempest import test
from tempest.services.keyvalue.json import magnetodb_client


class MagnetoDBTablesTestCase(test.BaseTestCase):

    def setUp(self, *args, **kwargs):
        super(MagnetoDBTablesTestCase, self).setUp()
        self.client = magnetodb_client.MagnetoDBClientJSON(None)

    def test_create_table(self):
        self.client.create_table({}, 'ololo', {}, {})

    def test_delete_table(self):
        self.client.delete_table('ololo')

    def test_list_tables(self):
        self.client.list_tables()

    def test_describe_table(self):
        self.client.describe_table('ololo')

    def test_put_item(self):
        self.client.put_item('ololo', {})

    def test_delete_item(self):
        self.client.delete_item('ololo', {})

    def test_get_item(self):
        self.client.get_item('ololo', {})

    def test_query(self):
        self.client.query('ololo', {})

    def test_scan(self):
        self.client.scan('ololo', {})

    def test_batch_get_item(self):
        self.client.batch_get_item({})

    def test_batch_write_item(self):
        self.client.batch_write_item({})
