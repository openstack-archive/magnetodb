from funkload.FunkLoadDocTest import FunkLoadTestCase
#from boto.dynamodb2.layer1 import DynamoDBConnection
#from boto.regioninfo import RegionInfo
import unittest
from tempest.thirdparty.boto import test_magnetodb_tables


class LoadTest2(test_magnetodb_tables.MagnetoDBTablesTest,
                FunkLoadTestCase):

    pass


if __name__ == "__main__":
    unittest.main()
    t = LoadTest2()
    t.test