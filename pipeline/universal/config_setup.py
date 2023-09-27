# Databricks notebook source
# MAGIC %run ./schemas

# COMMAND ----------

import os
path=os.getenv("configPath").strip()
spark.sql(f'create schema if not exists dbxConfig location "{path}"')

# COMMAND ----------

## probably need append not overwrite but wed have to pull and compare. probably want to ad in an ID column to to the dbxConfigTable. Schema is on the schemas notebook. 



df = spark.createDataFrame(values,schema=configTableSchema)
df.write.format("delta").mode("overwrite").saveAsTable("dbxConfig.dbxConfigTable")
