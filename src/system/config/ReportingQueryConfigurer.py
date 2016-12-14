# core
import os
import shutil

# MincePy
from AbstractQueryConfigurer import AbstractQueryConfigurer
from database.query.ReportingQueryFeature import ReportingQueryFeature
from system.task.output.XlsOutputHandler import XlsOutputHandler
from system.task.output.PptxOutputHandler import PptxOutputHandler
from system.task.output.MdbOutputHandler import MdbOutputHandler

class ReportingQueryConfigurer(AbstractQueryConfigurer):
    '''
    Creates instances of :class:`.ReportingQueryFeature` from configuration.
    
    :param system: reference to the MincePy system object
    :type system: :class:`.System`
    :param configHelper: reference to the configuration file helper
    :type configHelper: :class:`.ConfigHelper`
    :param overwrite: flag indicating whether or not to overwrite tables
        in reporting databases when the script is run:
        True:  the table pointed to by the reporting feature is dropped
        False: the table pointed to by the reporting feature is appended to
    '''
    
    def __init__(self, system, configHelper, overwrite):
        super(ReportingQueryConfigurer, self).__init__(system, configHelper)
        self.__overwrite = overwrite
        self.__queryConfigurers = {
            ".mdb"  : self.__configureMdbReport,
            ".accdb": self.__configureMdbReport,
            ".xlsx" : self.__configureXlsOutputHandler,
            ".xls"  : self.__configureXlsOutputHandler,
            ".pptx" : self.__configurePptxOutputHandler,
        }
    
    
    def configure(self, query, featureConfig):
        path = featureConfig.get("output_database") or featureConfig.get("file")
        _, ext = os.path.splitext(path)
        self.__queryConfigurers[ext](query, featureConfig)


    def __configureMdbReport(self, query, featureConfig):
        reportingDBPath = self.getConfigHelper().getAbsolutePath(
                featureConfig.get("output_database"))
        
        if not os.path.exists(reportingDBPath):
            self.getSystem().getDatabaseService().create(reportingDBPath)
        
        reportingTable = featureConfig.get("output_table")
        reportingDB = self.getSystem().getConnectionManager().open(path=reportingDBPath)
        if self.__overwrite and reportingDB.hasTable(reportingTable):
            reportingDB.execute("DROP TABLE [%s]" % reportingTable)
            
        dbTitleColumn = featureConfig.get("db_title_column")
        mode = featureConfig.get("mode")
        if mode == "native":
            self.__configureStandardReportingFeature(
                    query, reportingDBPath, reportingTable, dbTitleColumn)
        else:
            self.__configureMdbOutputHandler(
                    query, reportingDBPath, reportingTable, dbTitleColumn)
        
        
    def __configureStandardReportingFeature(
            self, query, dbPath, table, dbTitleColumn):

        feature = ReportingQueryFeature(
                self.getSystem().getConnectionManager(),
                dbPath,
                table,
                dbTitleColumn)
        
        query.addFeature(feature)
    
    
    def __configureMdbOutputHandler(self, query, dbPath, table, dbTitleColumn):
        handler = MdbOutputHandler(
                self.getSystem().getConnectionManager(),
                dbPath,
                table,
                dbTitleColumn)
        
        query.setOutputHandler(handler)
    
        
    def __configureXlsOutputHandler(self, query, featureConfig):
        outputPath = self.getConfigHelper().getAbsolutePath(featureConfig["file"])
        outputDir, _ = os.path.split(outputPath)
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        
        if self.__overwrite and os.path.exists(outputPath):
            os.unlink(outputPath)
        
        template = featureConfig.get("template")
        if template and not os.path.exists(outputPath):
            shutil.copyfile(template, outputPath)
        
        worksheet = featureConfig["worksheet"]
        cell = featureConfig.get("cell")
        header = featureConfig.get("header") or False
        
        handler = XlsOutputHandler(outputPath, worksheet, cell, header)
        query.setOutputHandler(handler)
        
        
    def __configurePptxOutputHandler(self, query, featureConfig):
        outputPath = self.getConfigHelper().getAbsolutePath(featureConfig["file"])
        outputDir, _ = os.path.split(outputPath)
        if not os.path.exists(outputDir):
            os.makedirs(outputDir)
        
        if self.__overwrite and os.path.exists(outputPath):
            os.unlink(outputPath)
        
        template = featureConfig.get("template")
        if template and not os.path.exists(outputPath):
            shutil.copyfile(template, outputPath)
        
        slideNum = int(featureConfig["slide"])
        shapeName = featureConfig["shape"]
        
        handler = PptxOutputHandler(outputPath, slideNum, shapeName)
        query.setOutputHandler(handler)
