# Databricks notebook source
# MAGIC %run ../universal_functions/environment_setup

# COMMAND ----------

def create_bronze_dataframe(fileName: str,dataLakeConfig: object):
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFileFrom(fileName)
    return df

def create_bronze_table(tableName: str, df ,dataLakeConfig: object):
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName)



    
