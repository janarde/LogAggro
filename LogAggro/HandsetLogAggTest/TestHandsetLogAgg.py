'''
Created on Aug 2, 2012

@author: Jesse_Anarde
'''
import unittest
from HandsetLogAgg import ConnectionProperties


class Test(unittest.TestCase):


    def setUp(self):
        
        self.propertyfile = 'C:\Users\Jesse_Anarde\git\LogAggro\LogAggro\\resources\HandsetLogAgg.properties'
        self.connProps = ConnectionProperties.ConnectionProperties(self.propertyfile)


    def tearDown(self):
        pass


    def test_parse_properties(self):
        
        self.assertNotEqual('', self.connProps.loadProperties())

        
    def test_get_connection_info(self):
        items = self.connProps.getConnectionInfo()
        self.assertIn('keyspace', items)
        self.assertIn('column_family', items)
        
    def test_get_ring_info(self):
        nodes = self.connProps.getRingInfo()
        print nodes
        self.assertIn('node1', nodes)
        self.assertIn('node2', nodes)  

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()