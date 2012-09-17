'''
Created on Aug 7, 2012

@author: Jesse_Anarde
'''
import unittest
from HandsetLogAgg import DataSourceConnection, ConnectionProperties


class Test(unittest.TestCase):




    def setUp(self):
        connProps = ConnectionProperties.ConnectionProperties('C:\Users\Jesse_Anarde\git\LogAggro\LogAggro\\resources\HandsetLogAgg.properties')
        self.ds = DataSourceConnection.DataSourceConnection(connProps)


    def tearDown(self):
        pass


    def test_get_connection_pool(self):
        
        pool = self.ds.getConnectionPool()
        self.assertNotEqual(pool, '')
    
    
    def test_get_column_family(self):
        cf = self.ds.getColumnFamily()
        self.assertNotEqual(cf, '')


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()