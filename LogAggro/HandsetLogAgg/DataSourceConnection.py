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
        def __init__(self):
            return self  
        
    def __init__(self, ConnectionProperties):
        if DataSourceConnection.__instance is None:
            #Create and remember the instance
            DataSourceConnection.__instance = DataSourceConnection.DataSourceConnectionImpl()
            
        # Store the instance reference as the only member in the handle
        self.__dict__['DataSourceConnection__instance'] = DataSourceConnection.__instance
        
        self.properties = {}
        self.properties = ConnectionProperties.ConnectionProperties

    def getConnection(self):
        KSInfo = self.properties.getConnectionInfo()
        ringInfo = self.properties.getRingInfo()
        
        connInfo = dict(KSInfo.items() + ringInfo.items())
        
        listcount = 0
        nodelist = []
        
        for key in connInfo.keys():
            
            keyspace = connInfo.get("keyspace")
            column_family = connInfo("column_family")
            
            if re.search('node', connInfo.get(key)):
                
                nodelist[listcount] = connInfo.get(key)
                listcount = listcount + 1
        
        
        pool = ConnectionPool(keyspace, nodelist)
        
        return pool 
             
        
        