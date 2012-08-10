'''
Created on Aug 1, 2012

@author: Jesse_Anarde
'''

from ConfigParser import SafeConfigParser


class ConnectionProperties(object):
    
    def __init__(self, propertyfile):
        
        self.propertyfile = propertyfile
        self.parser = SafeConfigParser()
    
    def loadProperties(self):
        
        return self.parser.read(self.propertyfile)
        
    
    def getConnectionInfo(self):
        Items = {}
        
        self.loadProperties()
        Items['keyspace'] = self.parser.get('connection', 'keyspace')
        Items['column_family'] = self.parser.get('connection', 'column_family')
        return Items
    
    def getRingInfo(self):
        Nodes = {}
        self.loadProperties()
        node_items = self.parser.items("nodes")
        
        for key, node in node_items:
            Nodes[key] = node
            
        return Nodes
        
    
    
        
        