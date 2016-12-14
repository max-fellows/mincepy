# core
import os
import re
import logging

# contrib
import sqlite3

# MincePy
from database.AbstractDatabase import AbstractDatabase
from database.SQLiteDatabaseCursor import SQLiteDatabaseCursor
from database.query.QueryError import QueryError
from dataimport.DataImportError import DataImportError

class SQLiteDatabase(AbstractDatabase):
    '''
    Holds a connection to a SQLite database.

    :param dbPath: full path to .db file, ex: ``r"c:\sdms\mydb.db"``
    :type dbPath: str
    :param connectParams: optional connection parameters
    :type connectParams: dict or None
    '''

    pyToSqlTypes = {"unicode"        : "TEXT",
                    "float"          : "REAL",
                    "decimal.Decimal": "REAL",
                    "int"            : "INTEGER",
                    "str"            : "TEXT",
                    "bool"           : "INTEGER",
                    "memo"           : "TEXT"}
    

    def __init__(self, path):
        self.__connection = None
        self.__dbPath = os.path.abspath(path)
        self.__importedTables = []
        self.__log = logging.getLogger("app.%s" % self.__class__.__name__)
        
        
    def __str__(self):
        return self.__dbPath
        

    def __connect(self, dbPath):
        '''
        Connects to a database.

        :param dbPath: full path to .db file, ex: ``r"c:\sdms\mydb.db"``
        :type dbPath: str
        :param connectParams: optional connection parameters
        :type connectParams: dict or None
        :rtype: connection
        '''
        try:
            connection = sqlite3.connect(dbPath)
        except sqlite3.Error as detail:
            raise Exception("Error connecting to %s: %s" % (dbPath, detail))
            
        return connection
    
    
    def __getConnection(self):
        '''
        Gets the active connection for this instance, opening a new one if
        necessary.

        :rtype: connection
        '''
        if self.__connection is None:
            self.__connection = self.__connect(self.__dbPath)
            
        return self.__connection
    
    
    def __cleanColumnNames(self, columnNames):
        # Non-alphanumeric characters are not allowed in database column names.
        return [re.sub("[^0-9a-zA-Z\*\-\ \(\)']+", "_", col)
                for col in columnNames]
    
    
    def __isSpace(self, value):
        if isinstance(value, str):
            return str(value).isspace() or str(value) == ""
        if isinstance(value, unicode):
            return unicode(value).isspace() or unicode(value) == ""
        return False
    
    
    def __initializeTable(self, tableName, columnNames, columnTypes):
        if tableName not in self.__importedTables:
            if self.hasTable(tableName):
                self.execute("DROP TABLE {}".format(tableName))

            colDefs = ",".join([
                    "{} {}".format(colName, SQLiteDatabase.pyToSqlTypes[colType])
                    for colName, colType in zip(columnNames, columnTypes)])
            
            createTableSql = "CREATE TABLE {} ({})".format(tableName, colDefs)
            self.execute(createTableSql)
            self.__importedTables.append(tableName)
    
    
    def __getMetadata(self, sql, params=None):
        firstRowSql = "SELECT * FROM ({}) LIMIT 1".format(sql)
        try:
            if params is not None:
                cur = self.__getConnection().cursor().execute(firstRowSql, params)
            else:
                cur = self.__getConnection().cursor().execute(firstRowSql)
        except sqlite3.Error as e:
            raise QueryError(e)

        description = cur.description
        firstRow = cur.fetchone()
        cols = [col[0] for col in description]
        types = [str(type(firstRow[i])).split("'")[1] for i in range(len(cols))]
        cur.close()
        
        return (cols, types)
    
    
    def close(self):
        '''
        Closes the database connection.
        '''
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = None
    
    
    def query(self, sql, params=None):
        '''
        Performs a read-only query.

        :param sql: SQL to execute
        :type sql: str
        :param params: SQL parameters
        :type params: list or None
        :rtype: list of rows
        '''
        try:
            if params is not None:
                cur = self.__getConnection().cursor().execute(sql, params)
            else:
                cur = self.__getConnection().cursor().execute(sql)
        except sqlite3.Error as e:
            raise QueryError(e)
        
        cols, types = self.__getMetadata(sql, params)
        
        return SQLiteDatabaseCursor(cur, cols, types)
    
        
    def execute(self, sql, params=None):
        ''' 
        Executes and commits an SQL statement.
        
        :param sql: SQL to execute
        :type sql: str
        :param params: SQL parameters
        :type params: list or None
        :returns: the number of rows affected
        :rtype: int
        '''
        try:
            if params is not None:
                rowCount = self.__getConnection().cursor().execute(
                        sql, params).rowcount
            else:
                rowCount = self.__getConnection().cursor().execute(sql).rowcount
        except sqlite3.Error as e:
            raise QueryError(e)
            
        self.__getConnection().commit()
        
        return rowCount
    

    def executeMany(self, sql, params):
        ''' 
        Executes an SQL statement once per set of parameters.
        
        :param sql: SQL insert/update/delete statement to repeat
        :type sql: str
        :param params: SQL parameter lists
        :type params: list of lists
        '''
        try:
            self.__getConnection().cursor().executemany(sql, params)
        except sqlite3.Error as e:
            raise QueryError(e)
        
        self.__getConnection().commit()
        
    
    def hasTable(self, tableName):
        '''
        Checks if a table exists in the database.

        :param name: table name to find
        :type name: str
        :returns: True if found, False if not
        :rtype: boolean
        '''
        try:
            return self.__getConnection().cursor().execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    [tableName]).fetchone() is not None
        except sqlite3.Error as e:
            raise QueryError(e)


    def importData(self, dataImport):
        '''
        Imports data into the database.
        
        :param dataImport: the DataImport to store data from
        :type dataImport: `.DataImport`
        '''
        tableName = dataImport.getDestination()
        titleColumn = dataImport.getTitleColumn()

        columnNames = list(dataImport.getColumns())
        if not columnNames:
            raise DataImportError(
                    "Error processing data import '{0}': No column names "
                    "specified. Either configure the column names explicitly or "
                    "ensure that the input data contains a header row.".format(
                            dataImport.getName()))
        
        if titleColumn:
            columnNames.insert(0, titleColumn)
        columnNames = self.__cleanColumnNames(columnNames)
        
        columnTypes = dataImport.getTypes()
        if titleColumn:
            columnTypes.insert(0, "str")

        self.__initializeTable(tableName, columnNames, columnTypes)

        insertSql = "INSERT INTO {table} ({columns}) VALUES ({placeholders})" \
            .format(table=tableName,
                    columns=",".join(columnNames),
                    placeholders=",".join("?" * len(columnNames)))
            
        batch = []
        for count, row in enumerate(dataImport.getRows(), 1):
            if titleColumn:
                row.insert(0, dataImport.getName())
                
            if len(row) > len(columnNames):
                raise DataImportError(
                        "Error processing data import '{0}': {1} columns "
                        "expected, but data contains {2}. Check configured "
                        "column names.".format(dataImport.getName(),
                                               len(columnNames), len(row)))
            
            data = []
            for value in row:
                if self.__isSpace(value):
                    data.append(None)
                data.append(value)
            batch.append(data)
            
            if count % 10000 == 0:
                if batch:
                    self.executeMany(insertSql, batch)
                    self.__log.info("Imported %d rows." % count)
                    batch = []
        
        if batch:
            self.executeMany(insertSql, batch)
            self.__log.info("Imported %d rows." % count)
