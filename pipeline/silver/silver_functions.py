# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

def createSilverDataframe(tableName: str, dataLakeConfig: object):
    assert "_bronze" == tableName[-7:], "tableName argument must contain _bronze suffix"
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFromTable(tableName)
     
    #example manipulations
    df = df.drop(*schema_diabetes_silver_drop_cols).dropDuplicates(diabetes_silver_identity_cols)
    
    return df

def createSilverTable(tableName: str,df: object, dataLakeConfig: object,loadType: str):
    assert "_silver" == tableName[-7:], "tableName argument must contain _silver suffix"
    assert loadType in ["full","incremental"], "loadType needs to be 'full' or 'incremental'"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName,loadType)
   
