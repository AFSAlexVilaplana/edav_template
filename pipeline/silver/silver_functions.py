# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

def createSilverDataframe(tableName: str, dataLakeConfig: object):

    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFromTable(tableName)
     
    #example manipulations
    df = df.drop(*schema_diabetes_silver_drop_cols).dropDuplicates(diabetes_silver_identity_cols)
    
    return df

def createSilverTable(tableName: str,df: object, dataLakeConfig: object):
    assert "_silver" == tableName[-7:], "tableName argument must contain _silver suffix"
    
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName)
   
