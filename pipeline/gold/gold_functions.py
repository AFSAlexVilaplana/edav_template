# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

def createGoldDataframe(tableName: str, dataLakeConfig: object):

    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFromTable(tableName)
     
    #example manipulations
    
    return df

def createGoldTable(tableName: str,df: object, dataLakeConfig: object):
    assert "_gold" == tableName[-5:], "tableName argument must contain _gold suffix"
    
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName)
   
