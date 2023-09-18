# Databricks notebook source
# MAGIC %run ./schemas

# COMMAND ----------

import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
#############################below to be implemented for jobs##########################
class templateEnvironment:
    database = dbutils.jobs.taskValues.get(taskKey = 'set_up_params',key="database",debugValue= os.getenv("database)"))
    database_folder = dbutils.jobs.taskValues.get(taskKey='set_up_params',key='database_folder',debugValue = os.getenv("database_folder"))
    scope_name = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "scope_name", debugValue = os.getenv("scope_name"))
    
dbutils.widgets.dropdown("read_file_path","abfss://s3t8templatedl@cmods3t8dleus.dfs.core.windows.net/",["abfss://s3t8templatedl@cmods3t8dleus.dfs.core.windows.net/"])
dbutils.widgets.dropdown("write_file_path","abfss://s3t8templatedl@cmods3t8dleus.dfs.core.windows.net/delta/",["abfss://s3t8templatedl@cmods3t8dleus.dfs.core.windows.net/delta/"])

# dbutils.widgets.dropdown("read_file_path","abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/",["abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/"])
# dbutils.widgets.dropdown("write_file_path","abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/delta/",["abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/delta/"])


class TemplateEnvironment:
    def __init__(self):
        self.__readFilePath = dbutils.widgets.get("read_file_path")
        self.__writeFilePath = dbutils.widgets.get("write_file_path")
        self.__database = os.getenv("database")
        self.__database_folder = os.getenv("database_folder")
    def getReadFilePath(self):
        return self.__readFilePath
    def getWriteFilePath(self):
        return self.__writeFilePath
    def getDatabase(self):
        return self.__database
    def getDatabaseFolder(self):
        return self.__database_folder
    
globalTemplateEnv = TemplateEnvironment()




# COMMAND ----------

class dataLakeConfig:
    
    def __init__(self,readFilePath,writeFilePath,dbName,rootDir):
        self.__readFilePath = readFilePath
        self.__writeFilePath = writeFilePath
        self.__dbName = dbName
        self.__rootDir = rootDir
    
    def getTable(self,tableName):
        
        return f"{self.__dbName}.{tableName}"
    
    def getWritePath(self,tableName):
        
        return f"{self.__writeFilePath}{tableName}/"
    
    def getReadPath(self,fileName):
        
        return f"{self.__readFilePath}{fileName}"
    
    def getDbName(self):

        return self.__dbName
    
    def getRootDir(self):
        
        return self.__rootDir
    
    
class dataLakeConnection:
    
    def __init__(self,dataLakeConfig):
        self.dataLakeConfig = dataLakeConfig
    
    def readFileFrom(self,fileName,fileformat,schema=''):
        assert fileformat in ['csv','delta','text','avro','json', 'parquet'], "arg must be one of ['csv','delta','text','avro','json','parquet']"
        if schema:
            return spark.read.format(fileformat.lower()).option("header","true").option("multiline","true").schema(schema).load(self.dataLakeConfig.getReadPath(fileName))
        else:
            return spark.read.format(fileformat.lower()).option("header","true").option("multiline","true").option("inferschema","true").load(self.dataLakeConfig.getReadPath(fileName))
        
    def writeFileToTable(self,df,tableName):
       
        return df.write.format("delta").mode("append").option("mergeSchema","true").save(self.dataLakeConfig.getWritePath(tableName))
    

    def readFromTable(self,tableName):
        
        return spark.read.format("delta").option("ignoreDeletes","true").table(self.dataLakeConfig.getTable(tableName))
    
    #depending on size it is probably best to write directly to delta file rather than pull in table data for comparison
    def writeToTable(self,df,tableName):

        return df.write.format("delta").mode("append").option("overwriteSchema","true").saveAsTable(self.dataLakeConfig.getTable(tableName))
    
    def overwriteTable(self,df,tableName):
        
        return df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(self.dataLakeConfig.getTable(tableName))
    






# COMMAND ----------


globalDataLakeConfig = dataLakeConfig(readFilePath=globalTemplateEnv.getReadFilePath()
                                      ,writeFilePath=globalTemplateEnv.getWriteFilePath()
                                      ,dbName = globalTemplateEnv.getDatabase()
                                      ,rootDir = globalTemplateEnv.getDatabaseFolder()
                                      )

