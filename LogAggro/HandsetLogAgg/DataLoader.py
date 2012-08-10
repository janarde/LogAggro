import os
import csv
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import hashlib


#CVS Parser utility class
class CSVParser:


    #===========================================================================
    # Constructor
    #===========================================================================

    def __init__(self, infile, inpath):
        self.infile = infile
        self.inpath = inpath
        

    #===================================================================
    # Informational 
    #===================================================================
    
    def __repr__(self):
        string = ["+++++++++++++++++++++++++",
                  "+" + self.infile,
                  "+" + self.inpath,
                  "+++++++++++++++++++++++++"]
        
        return "\n".join(string)
    
    #===================================================================
    # String representation of the class 
    #===================================================================    
    
    def __str__(self):
        return self.__repr__()
    
    #===================================================================
    # String representation of the class 
    #=================================================================== 
        
    def hashString(self, key):
        m = hashlib.new('md5', key)
        m.update(key)
        return m.hexdigest()
        
    def insert(self, row):
        
        pool = ConnectionPool('HandsetLogKS', ['batt1.nuance.com:9160', 'batt2.nuance.com:9160'])
        col_fam = ColumnFamily(pool, 'HandsetLogEntriesCF')
        
        
        if len(row) >= 1:
            print row
            key = self.hashString(row['s-ip']+row['c-query'])
            col_fam.insert(key, row)
            
             

        
    def parseFile(self):
        filename = os.path.join(self.inpath, self.infile)
        data = csv.reader(open(filename), delimiter=' ', skipinitialspace=True)
        
        fields = data.next()
        #print "fields = "
        print fields
        
        for row in data:
            #print "row = "
            #print row
            items = zip(fields, row)
            item = {}
            
            
            for (name, value) in items:
                item[name] = value.strip()
                
            self.insert(item)
                
    def main(self):
        
        print "stuff"
        print self.__repr__()
        
        #self.__init__("test.csv", "stuff.csv", "C:\working\cassandra\\", "C:\working\cassandra\\")
        
        
        self.parseFile()
        

if __name__ == '__main__':
    
    CSVParser("test.csv", "C:\working\cassandra\\").main()
        
        
        
        
        
        
        
        
