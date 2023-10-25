# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

#might be best to leave dataframe manipulations to their respective notebooks because cases such as aggregating data would drop ingestion date unless specified in groupBy

#functions could have the "generic" transformations located in createGoldDataframe or in their respective notebooks

def createGoldDataframe(tableName: str, dataLakeConfig: object,schema: str = ''):
    assert "_silver" == tableName[-7:], "tableName argument must contain _silver suffix"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFromTable(tableName)

    df = add_ingestion_date(df)
     
        
    return df

def createGoldTable(tableName: str,df: object, dataLakeConfig: object,loadType: str):
    assert "_gold" == tableName[-5:], "tableName argument must contain _gold suffix"
    assert loadType in ["full","incremental"], "loadType needs to be 'full' or 'incremental'"
    


    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName,loadType)
   
