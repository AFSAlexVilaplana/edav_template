# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

fileName = dbutils.widgets.get("file_name")
fileExt = dbutils.widgets.get("file_ext")
tableName = dbutils.jobs.taskValues.get(taskKey = "set_up_params", key = "table_name")

parameters = specificTaskParameters(fileName,fileExt,tableName)

# COMMAND ----------



df = createBronzeDataframe(parameters.getFileName(),globalDataLakeConfig,parameters.getFileExt())

createBronzeTable(parameters.getTableNamePrefix()+'_bronze',df,globalDataLakeConfig)



