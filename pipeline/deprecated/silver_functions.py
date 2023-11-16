# Databricks notebook source
# MAGIC %run ../universal/environment_setup

# COMMAND ----------

#might be best to leave dataframe manipulations to their respective notebooks because cases such as aggregating data would drop ingestion date unless specified in groupBy.
#functions could have the "generic" transformations located in createSilverDataframe or in their respective notebooks

def createSilverDataframe(tableName: str, dataLakeConfig: object,schema: str='', dropCols: list='', identityCols: list=''):
    assert "_bronze" == tableName[-7:], "tableName argument must contain _bronze suffix"
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFromTable(tableName)
    if dropCols[0] and identityCols[0]:
        df = drop_cols_and_dupes(df,dropCols,identityCols)
    df = add_ingestion_date(df)
    
    return df

def createSilverTable(tableName: str,df: object, dataLakeConfig: object,loadType: str):
    assert "_silver" == tableName[-7:], "tableName argument must contain _silver suffix"
    assert loadType in ["full","incremental"], "loadType needs to be 'full' or 'incremental'"
    

    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName,loadType)
   
