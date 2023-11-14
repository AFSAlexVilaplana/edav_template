# Databricks - Create Dataframes and Tables on ADLS

# MAGIC %run ./environment_setup

# COMMAND ----------

def createDataframe(medallion_step: str, source: str, dataLakeConfig: object, fileFormat: str = '', schema: str=''):
    #refer to bronze_functions notebook for arguments
    #needs to accept arguments for file and table\
    #maybe better to be split into two functions

    assert medallion_step in ['bronze','silver','gold'], "medallion step must be one item of this list ['bronze','silver','gold']"

    dataLakeConn = dataLakeConnection(dataLakeConfig,schema)

    if medallion_step == 'bronze':
        return dataLakeConn.readFileFrom(sourceName = source, 
                                         fileFormat = fileFormat, 
                                         schema = schema)
    else:
        return dataLakeConn.readFromTable(tableName = source)

def createTable( dataLakeConfig : object, df : object, tableName : str, medallion_step : str, load_type : str = 'incremental'):     #add arguments based on bronze notebook
    """
    loadType in ["full","incremental"], "loadType must be 'full' or 'incremental'"
    medallion_step in ['bronze','silver','gold'], "medallion step must be one item of this list ['bronze','silver','gold']"
    """
    tableDest = tableName + "_" + medallion_step

    dataLakeConn = dataLakeConnection(dataLakeConfig)
    dataLakeConn.writeToTable(df, tableDest, loadType)

# COMMAND --------

# Streaming Logic

def createStreamingDataframe(dataLakeConfig,schema):     
    #arguments should be the same as createDataframe
    #need to create readstream method on datalakeConnection class in environment_setup
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    df = dataLakeConn.readFileFrom(fileName,fileFormat,schema)


def createStreamTable(medallion_step,dataLakeConfig, load_type = ''): # should have similar arugments to createTable
    assert medallion_step in ['bronze','silver','gold'],"medallion step must be one item of this list ['bronze','silver','gold']"
    assert loadType == 'autoloader', "loadType must be autoloader"
    
    dataLakeConn = dataLakeConnection(dataLakeConfig)
    #need to create writeStreamToTable method below
    #dataLakeConn.writeStreamToTable(df,tableName,loadType)
    