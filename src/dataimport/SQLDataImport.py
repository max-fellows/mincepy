# MincePy
from DataImport import DataImport

class SQLDataImport(DataImport):
    '''
    Imports data from a database.
    '''
    
    def __init__(self, name, destination, db, query, titleColumn=None,
                 nullValues=None, dbNull=None):
        self.__name = name
        self.__destination = destination
        self.__titleColumn = titleColumn
        self.__nullValues = nullValues or []
        self.__dbNull = dbNull
        self.__db = db
        self.__query = query
        self.__columns = None
        self.__types = None
        self.__cur = None
        
    
    def __getCursor(self):
        if not self.__cur:
            self.__cur = self.__db.query(self.__query)
        
        return self.__cur 
    
    
    def getName(self):
        '''
        See :meth:`.DataImport.getName`.
        '''
        return self.__name
    
    
    def getDestination(self):
        '''
        See :meth:`.DataImport.getDestination`.
        '''
        return self.__destination
    
    
    def getTitleColumn(self):
        '''
        See :meth:`.DataImport.getTitleColumn`.
        '''
        return self.__titleColumn
    
    
    def getColumns(self):
        '''
        See :meth:`.DataImport.getColumns`.
        '''
        if not self.__columns:
            cur = self.__getCursor()
            self.__columns = cur.getColumns()
            
        return self.__columns
    
    
    def getRows(self):
        '''
        See :meth:`.DataImport.getRows`.
        '''        cur = self.__getCursor()        for row in cur:
            yield [row[i]
                   if row[i] is not None and unicode(row[i]) not in self.__nullValues
                   else self.__dbNull
                   for i in range(len(self.getColumns()))]
        
        self.__cur = None


    def getTypes(self):
        '''
        See :meth:`.DataImport.getTypes`.
        '''
        if not self.__types:
            cur = self.__getCursor()
            self.__types = cur.getTypes()
        
        return self.__types
