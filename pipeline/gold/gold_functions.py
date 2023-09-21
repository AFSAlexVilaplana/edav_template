# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

def createGoldDataframe(tableName: str, dataLakeConfig: object):
    assert "_silver" == tableName[-7:], "tableName argument must contain _silver suffix"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFromTable(tableName)
     
        
    return df

def createGoldTable(tableName: str,df: object, dataLakeConfig: object,loadType: str):
    assert "_gold" == tableName[-5:], "tableName argument must contain _gold suffix"
    assert loadType in ["full","incremental"], "loadType needs to be 'full' or 'incremental'"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName,loadType)
   
