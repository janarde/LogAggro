'''
Created on Aug 1, 2012

@author: Jesse_Anarde
'''

import re
import sys
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

class DataSourceConnection(object):

# Trying to do a singleton implementation

    __instance = None

    class DataSourceConnectionImpl:  
        def __call__(self):
            return self  
        
    def __init__(self, ConnectionProperties):
        if DataSourceConnection.__instance is None:
            #Create and remember the instance
            DataSourceConnection.__instance = DataSourceConnection.DataSourceConnectionImpl()
            
        # Store the instance reference as the only member in the handle
        self.__dict__['DataSourceConnection__instance'] = DataSourceConnection.__instance
        
        self.properties = {}
        self.properties = ConnectionProperties
        KSInfo = self.properties.getConnectionInfo()
        ringInfo = self.properties.getRingInfo()
        
        connInfo = dict(KSInfo.items() + ringInfo.items())
        
        self.nodelist = []
        
        for key in connInfo.keys():
            
            self.keyspace = connInfo.get("keyspace")
            self.column_family = connInfo.get("column_family")
            
            print 'about to look for nodes'
            
            if re.search('node*', key):
                print connInfo.get(key)
                self.nodelist.append(connInfo.get(key))
                

    def getConnectionPool(self):        
        
        try:
            
            pool = ConnectionPool(self.keyspace, self.nodelist)
            return pool
        except:
            e = sys.exc_info()[0]
            print e
            
        
    
    def getColumnFamily(self):
        col_fam = ColumnFamily(self.getConnectionPool(), self.column_family)
        
        return col_fam
    
    
        