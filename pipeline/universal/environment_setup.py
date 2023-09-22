# Databricks notebook source
# MAGIC %run ./schemas

# COMMAND ----------

import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
import functools

# dbutils.widgets.removeAll()
# dbutils.widgets.dropdown("read_file_path",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
# dbutils.widgets.dropdown("write_file_path",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
# dbutils.widgets.dropdown("initial_task_key","set_up_params",["set_up_params"])
# dbutils.widgets.dropdown("specific_folder_path","healthcare-diabetes/",["healthcare-diabetes/"])
setupTaskKey = dbutils.widgets.get("initial_task_key")



class TemplateEnvironment:
    def __init__(self):
        self.__readFilePath = dbutils.jobs.taskValues.get(taskKey = setupTaskKey,key="readFilePath",debugValue= os.getenv("read_file_path").strip())
        self.__database = dbutils.jobs.taskValues.get(taskKey = setupTaskKey,key="database",debugValue= os.getenv("database"))
        self.__database_folder = dbutils.jobs.taskValues.get(taskKey = setupTaskKey,key='database_folder',debugValue = os.getenv("database_folder"))
        self.__scope = dbutils.jobs.taskValues.get(taskKey = setupTaskKey, key = "scope_name", debugValue = os.getenv("scope_name"))

    def getReadFilePath(self):
        return self.__readFilePath
    def getDatabase(self):
        return self.__database
    def getDatabaseFolder(self):
        return self.__database_folder


    

class persistantTaskParameters:
    def __init__(self):
        self.__loadType = dbutils.jobs.taskValues.get(taskKey = setupTaskKey, key = "load_type",debugValue="full")
        self.__sourceName = dbutils.jobs.taskValues.get(taskKey = setupTaskKey, key = "source_name",debugValue="healthcare-diabetes/")
        self.__fileExt = dbutils.jobs.taskValues.get(taskKey = setupTaskKey, key = "file_ext",debugValue="csv")
        self.__tableName = dbutils.jobs.taskValues.get(taskKey = setupTaskKey, key = "dest_table_prefix",debugValue = 'diabetestest')

        
    def getloadType(self):
        return self.__loadType
        
    def getSourceName(self):
        return self.__sourceName
    
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
    
    def getFilePath(self,fileName):
        
        return f"{self.__readFilePath}{fileName}"
    
    def getDbName(self):

        return self.__dbName
    
    def getRootDir(self):
        
        return self.__rootDir
    
    def getReadFilePath(self):
        
        return self.__readFilePath
    
    
    
class dataLakeConnection:
    
    def __init__(self,dataLakeConfig):
        self.dataLakeConfig = dataLakeConfig
    
    def readFileFrom(self,sourceName,fileFormat,schema):
        assert fileFormat in ['csv','delta','text','avro','json', 'parquet'], "arg must be one of ['csv','delta','text','avro','json','parquet']"
        
        newFileRootPath = self.dataLakeConfig.getReadFilePath()+sourceName
        newFileFolder = newFileRootPath + sorted([x.name for x in dbutils.fs.ls(newFileRootPath)],reverse=True)[0]
        
        if fileFormat != "delta":
            newFiles = dbutils.fs.ls(newFileFolder)
            
            newFileList = [x.name for x in newFiles]
           

            combineDf = functools.reduce(lambda df,df1: df.union(df1),
                        [spark.read.format(fileFormat.lower()).option("header","true").option("multiline","true").schema(schema).load(newFileFolder+newFile) for newFile in newFileList])
            return combineDf

        
        df = spark.read.format(fileFormat.lower()).option("header","true").option("multiline","true").schema(schema).load(newFileFolder)

    
    def readFromTable(self,tableName):
        
        return spark.read.format("delta").option("ignoreDeletes","true").table(self.dataLakeConfig.getTable(tableName))
    

    def writeToTable(self,df,tableName,load_type):
        if load_type == "full":
            return df.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(self.dataLakeConfig.getTable(tableName))
        else:
            return df.write.format("delta").mode("append").option("overwriteSchema","true").saveAsTable(self.dataLakeConfig.getTable(tableName))
    
    






# COMMAND ----------


globalTemplateEnv = TemplateEnvironment()



globalDataLakeConfig = dataLakeConfig(readFilePath=globalTemplateEnv.getReadFilePath()
                                      ,dbName = globalTemplateEnv.getDatabase()
                                      ,rootDir = globalTemplateEnv.getDatabaseFolder()
                                      )

globalPersistentTaskParameters = persistantTaskParameters()


