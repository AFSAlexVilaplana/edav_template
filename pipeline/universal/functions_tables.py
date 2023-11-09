# Databricks - Create Dataframes and Tables on ADLS

# MAGIC %run ./environment_setup

# COMMAND ----------

def createDataframe(fileName: str, dataLakeConfig: object, fileFormat: str, schema: str=''):
    #refer to bronze_functions notebook for arguments
    #needs to accept arguments for file and table\
    #maybe better to be split into two functions

    dataLakeConn = dataLakeConnection(dataLakeConfig,schema)
    df = dataLakeConn.readFileFrom(fileName, fileFormat,schema)
    
    return df

def createTable(medallion_step, load_type : str, dataLakeConfig : object, df : object, tableName : str):     #add arguments based on bronze notebook
    assert medallion_step in ['bronze','silver','gold'],"medallion step must be one item of this list ['bronze','silver','gold']"
    assert loadType in ["full","incremental"], "loadType must be 'full' or 'incremental'"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df,tableName,loadType)

# COMMAND --------

# Streaming Logic

def createStreamingDataframe(dataLakeConfig,schema):     
    #arguments should be the same as createDataframe
    #need to create readstream method on datalakeConnection class in environment_setup
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFileFrom(fileName,fileFormat,schema)


def createStreamTable(medallion_step,load_type,dataLakeConfig): # should have similar arugments to createTable
    assert medallion_step in ['bronze','silver','gold'],"medallion step must be one item of this list ['bronze','silver','gold']"
    assert loadType == 'autoloader', "loadType must be autoloader"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    #need to create writeStreamToTable method below
    #dataLakeConn.writeStreamToTable(df,tableName,loadType)
    