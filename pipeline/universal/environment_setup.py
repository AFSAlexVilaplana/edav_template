# Databricks notebook source
# MAGIC %run ./schemas

# COMMAND ----------

import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


dbutils.widgets.dropdown("read_file_path",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
dbutils.widgets.dropdown("write_file_path",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])



class TemplateEnvironment:
    def __init__(self):
        self.__readFilePath = dbutils.jobs.taskValues.get(taskKey = 'set_up_params',key="readFilePath",debugValue= os.getenv("read_file_path").strip())
        self.__database = dbutils.jobs.taskValues.get(taskKey = 'set_up_params',key="database",debugValue= os.getenv("database"))
        self.__database_folder = dbutils.jobs.taskValues.get(taskKey='set_up_params',key='database_folder',debugValue = os.getenv("database_folder"))
        self.__scope = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "scope_name", debugValue = os.getenv("scope_name"))
        

    def getReadFilePath(self):
        return self.__readFilePath
    def getDatabase(self):
        return self.__database
    def getDatabaseFolder(self):
        return self.__database_folder

    
globalTemplateEnv = TemplateEnvironment()


class specificTaskParameters(fileName,fileExt,tableName):
    def __init__(self):
        self.__fileName = fileName
        self.__fileExt = fileExt
        self.__tableName = tableName
        

    def getFileName(self):
        return self.__fileName
    
    def getFileExt(self):
        return self.__fileExt
    
    def getTableNamePrefix(self):
        return self.__tableName

# COMMAND ----------

class dataLakeConfig:
    
    def __init__(self,readFilePath,dbName,rootDir):
        self.__readFilePath = readFilePath
        self.__dbName = dbName
        self.__rootDir = rootDir     

    def getTable(self,tableName):
        
        return f"{self.__dbName}.{tableName}"
    
    def getWritePath(self,tableName):
        
        return f"{self.__rootDir}{tableName}/"
    
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
                                      ,dbName = globalTemplateEnv.getDatabase()
                                      ,rootDir = globalTemplateEnv.getDatabaseFolder()
                                      )

