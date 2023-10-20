# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame
import json

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
