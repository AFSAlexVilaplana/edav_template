# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

def createBronzeDataframe(fileName: str,dataLakeConfig: object,fileFormat: str,schema: object = ''):
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFileFrom(fileName,fileFormat,schema)
    
    return df

def createBronzeTable(tableName: str, df: object ,dataLakeConfig: object):
    assert "_bronze" == tableName[-7:], "tableName argument must contain _bronze suffix"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName)

        




    
