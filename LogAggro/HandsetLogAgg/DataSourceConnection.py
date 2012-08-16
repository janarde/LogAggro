'''
Created on Aug 1, 2012

@author: Jesse_Anarde
'''

import ConnectionProperties
import re
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

    def getConnectionPool(self):
        KSInfo = self.properties.getConnectionInfo()
        ringInfo = self.properties.getRingInfo()
        
        connInfo = dict(KSInfo.items() + ringInfo.items())
        
        nodelist = []
        
        for key in connInfo.keys():
            
            keyspace = connInfo.get("keyspace")
            #column_family = connInfo.get("column_family")
            
            print 'about to look for nodes'
            
            if re.search('node*', key):
                print connInfo.get(key)
                nodelist.append(connInfo.get(key))
                
        
        
        pool = ConnectionPool(keyspace, nodelist)
        
        return pool
        
        