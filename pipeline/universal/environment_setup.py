# Databricks notebook source
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,widgets are used for debugging
########run below for testing


#dbutils.widgets.removeAll()
dbutils.widgets.dropdown("readFilePath",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
dbutils.widgets.dropdown("databaseFolder",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
#dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
dbutils.widgets.dropdown("database",os.getenv("database"),[f"{os.getenv('database')}"])
dbutils.widgets.dropdown("sourceName","constructor/",["constructor/"])
dbutils.widgets.dropdown("fileExt","json",["json"])
dbutils.widgets.dropdown("loadType","full",["full","incremental"])
dbutils.widgets.dropdown("destTablePrefix","constructor984",["constructor984"])
dbutils.widgets.multiselect("dropColumns","url",["url","constructorRef"])
dbutils.widgets.multiselect("identityColumns","constructorId",['constructorId'])
dbutils.widgets.dropdown("silverCustomNotebookPath","/Repos/sebastian.clavijo@accenturefederal.com/edav_template/pipeline/silver/silver_constructor_execute",["/Repos/sebastian.clavijo@accenturefederal.com/edav_template/pipeline/silver/silver_constructor_execute"])
dbutils.widgets.dropdown("goldCustomNotebookPath","/Repos/sebastian.clavijo@accenturefederal.com/edav_template/pipeline/gold/gold_constructor_execute",["/Repos/sebastian.clavijo@accenturefederal.com/edav_template/pipeline/gold/gold_constructor_execute"])
dbutils.widgets.dropdown("bronzeCustomNotebookPath","",[""])


# dbutils.widgets.removeAll()
# dbutils.widgets.dropdown("readFilePath",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
# dbutils.widgets.dropdown("databaseFolder",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
# #dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
# dbutils.widgets.dropdown("database",os.getenv("database"),[f"{os.getenv('database')}"])
# dbutils.widgets.dropdown("sourceName","healthcare-diabetes/",["healthcare-diabetes/"])
# dbutils.widgets.dropdown("fileExt","csv",["csv"])
# dbutils.widgets.dropdown("loadType","full",["full","incremental"])
# dbutils.widgets.dropdown("destTablePrefix","diabete900",["diabete900"])
# dbutils.widgets.multiselect("dropColumns","BMI",["BMI","DiabetesPedigreeFunction"])
# dbutils.widgets.multiselect("identityColumns","id",['id'])
# dbutils.widgets.dropdown("silverCustomNotebookPath","",[""])
# dbutils.widgets.dropdown("goldCustomNotebookPath","",[""])
# dbutils.widgets.dropdown("bronzeCustomNotebookPath","",[""])




# COMMAND ----------

# DBTITLE 1,uncomment these widgets for full pipeline execution (including adf)
#####run below for actual job
# dbutils.widgets.removeAll()
# dbutils.widgets.dropdown("readFilePath",'',[""])
# dbutils.widgets.dropdown("databaseFolder",'',[''])
# #dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
# dbutils.widgets.dropdown("database",'',[""])
# dbutils.widgets.dropdown("sourceName","",[""])
# dbutils.widgets.dropdown("fileExt","",[""])
# dbutils.widgets.dropdown("loadType","",[""])
# dbutils.widgets.dropdown("destTablePrefix","",[""])
# dbutils.widgets.multiselect("dropColumns","",[""])
# dbutils.widgets.multiselect("identityColumns","",[''])
# dbutils.widgets.dropdown("silverCustomNotebookPath","",[""])
# dbutils.widgets.dropdown("goldCustomNotebookPath","",[""])
# dbutils.widgets.dropdown("bronzeCustomNotebookPath","",[""])




# COMMAND ----------

class TemplateEnvironment:
    def __init__(self):
        # self.__readFilePath = dbutils.widgets.get("readFilePath")
        # self.__database = dbutils.widgets.get("database")
        # self.__database_folder = dbutils.widgets.get("databaseFolder")
        # self.__scope = dbutils.widgets.get("scope")
        # self.__loadType = dbutils.widgets.get("loadType")
        # self.__sourceName = dbutils.widgets.get("sourceName")
        # self.__fileExt = dbutils.widgets.get("fileExt")
        # self.__tableName = dbutils.widgets.get("destTablePrefix")

        
        self.__database = dbutils.widgets.get("database")
        self.__databaseFolder = dbutils.widgets.get("databaseFolder")
        self.__sourceName = dbutils.widgets.get("sourceName")
        self.__readFilePath = dbutils.widgets.get("readFilePath").strip()
        # dbutils.jobs.taskValues.get(taskKey = setupTaskKey,key = "silverSchema", debugValue = dbutils.widgets.get("silverSchema"))
        # dbutils.jobs.taskValues.get(taskKey = setupTaskKey,key = "goldSchema", debugValue = dbutils.widgets.get("goldSchema"))
        self.__fileExt = dbutils.widgets.get("fileExt")
        self.__destTablePrefix = dbutils.widgets.get("destTablePrefix")
        self.__loadType = dbutils.widgets.get("loadType")
        self.__dropColumns = dbutils.widgets.get("dropColumns")
        self.__identityColumns = dbutils.widgets.get("identityColumns")
        #dbutils.jobs.taskValues.get(taskKey = setupTaskKey,key = "bronzeCustomNotebookPath", debugValue = dbutils.widgets.get("bronzeCustomNotebookPath"))
        self.__silverCustomNotebookPath = dbutils.widgets.get("silverCustomNotebookPath")
        self.__goldCustomNotebookPath = dbutils.widgets.get("goldCustomNotebookPath")
        self.__bronzeCustomNotebookPath = dbutils.widgets.get("bronzeCustomNotebookPath")

        
    def getDatabase(self):
        return self.__database
    
    def getDatabaseFolder(self):
        return self.__databaseFolder
    
    def getSourceName(self):
        return self.__sourceName
    
    def getReadFilePath(self):
        return self.__readFilePath
    
    def getFileExt(self):
        return self.__fileExt

    def getDestTablePrefix(self):
        return self.__destTablePrefix
    
    def getloadType(self):
        return self.__loadType
    
    def getDropColumns(self):
        return self.__dropColumns
    
    def getIdentityColumns(self):
        return self.__identityColumns
    
    def getSilverCustomNotebookPath(self):
        return self.__silverCustomNotebookPath
    
    def getGoldCustomNotebookPath(self):
        return self.__goldCustomNotebookPath
    
    def getBronzeCustomNotebookPath(self):
        return self.__bronzeCustomNotebookPath
    
    def setSilverCustomNotebookPath(self,path: str):
        self.__silverCustomNotebookPath = path
    
    def setGoldCustomNotebookPath(self, path: str):
        self.__goldCustomNotebookPath = path
    
    def setBronzeCustomNotebookPath(self,path: str):
        self.__bronzeCustomNotebookPath = path



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
    
    def readFileFrom(self,sourceName,fileFormat,schema=''):
        fileFormat = fileFormat.lower()
        assert fileFormat in ['csv','delta','text','avro','json', 'parquet'], "arg must be one of ['csv','delta','text','avro','json','parquet']"
        
        newFileRootPath = self.dataLakeConfig.getReadFilePath()+sourceName
        newFileFolder = newFileRootPath + sorted([x.name for x in dbutils.fs.ls(newFileRootPath)],reverse=True)[0]

        if schema:

            df = spark.read.format(fileFormat.lower()).option("header","true").schema(schema).load(newFileFolder)

        else:

             df = spark.read.format(fileFormat.lower()).option("header","true").option("inferSchema","true").load(newFileFolder)

        return df
    

    def readFromTable(self,tableName, schema = ''):
        """
            Return Table with schema. If no schema provided, infer schema
        """
        if schema:
            return spark.read.format("delta").option("ignoreDeletes","true").schema(schema).table(self.dataLakeConfig.getTable(tableName))
        else:
            return spark.read.format("delta").option("ignoreDeletes","true").option("inferSchema","true").table(self.dataLakeConfig.getTable(tableName))

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



