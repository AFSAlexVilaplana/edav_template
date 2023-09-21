# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

def createBronzeDataframe(fileName: str,dataLakeConfig: object,fileFormat: str,schema: object = ''):
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFileFrom(fileName,fileFormat,schema)
    
    return df

def createBronzeTable(tableName: str, df: object ,dataLakeConfig: object,loadType: str):
    assert "_bronze" == tableName[-7:], "tableName argument must contain _bronze suffix"
    assert loadType in ["full","incremental"], "loadType needs to be 'full' or 'incremental'"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName,loadType)

        




    
