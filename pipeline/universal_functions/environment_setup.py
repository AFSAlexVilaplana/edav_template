# Databricks notebook source
import os
#############################below to be implemented for jobs##########################
# class templateEnvironment:
#     database = dbutils.jobs.taskValues.get(taskKey = 'set_up_params',key="database",debugValue= os.getenv("database)"))
#     database_folder = dbutils.jobs.taskValues.get(taskKey='set_up_params',key='database_folder',debugValue = os.getenv("database_folder"))
#     scope_name = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "scope_name", debugValue = os.getenv("scope_name"))
#     gold_output_database = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "gold_output_database", debugValue = os.getenv("gold_output_database"))
#     #gold_output_database_checkpoint_prefix = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "gold_output_database_checkpoint_prefix", debugValue = os.getenv("gold_output_database_checkpoint_prefix"))
#     gold_database_folder = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "gold_database_folder", debugValue = os.getenv("gold_database_folder"))

dbutils.widgets.dropdown("read_file_path","abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/",["abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/"])
dbutils.widgets.dropdown("write_file_path","abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/delta/",["abfss://ocio-dex-db-dev@hl7dexdlg2.dfs.core.windows.net/delta/"])


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
        self.dbName = dbName
        self.rootDir = rootDir
    
    def getTable(self,tableName):
        
        return f"{self.dbName}.{tableName}"
    def getWritePath(self,tableName):
        
        return f"{self.__writeFilePath}{tableName}/"
    def getReadPath(self,fileName):
        
        return f"{self.__readFilePath}{fileName}"
    
class dataLakeConnection:
    
    def __init__(self,dataLakeConfig):
        self.dataLakeConfig = dataLakeConfig
    
    def readFileFrom(self,fileName):

        return spark.read.format("csv").option("header","true").load(self.dataLakeConfig.getReadPath(fileName))
    
    def writeFileToTable(self,df,tableName):
       
        return df.write.format("delta").mode("append").option("mergeSchema","true").save(self.dataLakeConfig.getWritePath(tableName))
    


    def readFromTable(self,tableName):
        
        return spark.read.format("delta").option("ignoreDeletes","true").table(self.dataLakeConfig.getTable(tableName))
    
    #depending on size it is probably best to write directly to delta file rather than pull in table data for comparison
    def writeToTable(self,df,tableName):

        return df.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(self.dataLakeConfig.getTable(tableName))





# COMMAND ----------


globalDataLakeConfig = dataLakeConfig(readFilePath=globalTemplateEnv.getReadFilePath()
                                      ,writeFilePath=globalTemplateEnv.getWriteFilePath()
                                      ,dbName = globalTemplateEnv.getDatabase()
                                      ,rootDir = globalTemplateEnv.getDatabaseFolder()
                                      )

