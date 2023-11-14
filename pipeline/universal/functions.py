# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
import json

# COMMAND ----------

def createDataframe(medallion_step: str, 
                    source: str, 
                    dataLakeConfig: object, 
                    fileFormat: str = '', 
                    schema: str=''):
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

def createTable( 
      dataLakeConfig : object, 
      df : object, 
      tableName : str, 
      medallion_step : str, 
      load_type : str = 'incremental'):     #add arguments based on bronze notebook
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

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list



# COMMAND ----------

# MAGIC %md
# MAGIC ###generic transformations to be utilized by any notebook

# COMMAND ----------

#example "generic" function below
def drop_cols_and_dupes(df: object,drop_cols: list,identity_cols: list):
    assert len(drop_cols) > 0, "drop columns cannot be empty"
    assert len(identity_cols) > 0, "identity columns cannot be empty"
    df = df.drop(*drop_cols).dropDuplicates(identity_cols)
    return df


def add_ingestion_date(input_df):
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

# MAGIC %md Multiline and single json files

# COMMAND ----------

# def load_json(file_path: str) -> DataFrame:
#     """Load JSON data from a file, whether it's multi-line or single-line."""

#     with open(file_path, 'r') as f:
#         content = f.readlines()

#     #Check if the JSON is nulti-line
#     is_multi_line = len(content) > 1 or any([line.strip().startswith('{') for line in content])

#     if is_multi_line:
#         # load as multi-line JSON
#         return spark.read.option("multiline", "true").json(file_path)
#     else:
#         #load as single-line JSON
#         return spark.read.json(file_path)
