# MincePy
from DataImport import DataImport

# contrib
from dbfpy.dbf import Dbf

# core
from contextlib import contextmanager

class DBFDataImport(DataImport):
    '''
    Imports data from a DBF database (usually a shapefile).
    
    :param name: the default name of the destination table
    :type name: str
    :param path: the absolute path to the file to import
    :type path: str
    :param destination: the name of the table to import data into
    :type destination: str
    :param titleColumn: the name of the identifier column if multiple data
        imports are being stored in the same table
    :type titleColumn: str
    '''
    
    dbfToPythonTypes = {
        "C": "unicode",
        "N": "int",
        "F": "float"
    }
    
    
    def __init__(self, name, path, destination, titleColumn=None):
        self.__name = name
        self.__path = path
        self.__destination = destination
        self.__titleColumn = titleColumn
        self.__columns = None
        self.__types = None
    
    
    @contextmanager
    def __openFile(self):
        try:
            db = Dbf(self.__path)
            yield db
        finally:
            db.close()
    
    
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
            with self.__openFile() as db:
                self.__columns = [field.name for field in db.header.fields]

        return self.__columns
    
    
    def getRows(self):
        '''
        See :meth:`.DataImport.getRows`.
        '''        with self.__openFile() as db:
            for row in db:
                yield row.asList()


    def getTypes(self):
        '''
        See :meth:`.DataImport.getTypes`.
        '''
        if not self.__types:
            with self.__openFile() as db:
                self.__types = [DBFDataImport.dbfToPythonTypes[field.typeCode]
                                  for field in db.header.fields]

        return self.__types
