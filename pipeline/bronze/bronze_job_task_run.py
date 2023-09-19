# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

fileName = dbutils.widgets.get("file_name")
fileExt = dbutils.widgets.get("file_ext")
tableName = dbutils.widgets.get("table_name")
print(dbutils.entry_point.getDbutils().notebook().getContext().notebookTaskMetadata.get("prevTask").get("name"))
parameters = taskParameters(fileName,fileExt,tableName)

# COMMAND ----------



df = createBronzeDataframe(parameters.getFileName(),globalDataLakeConfig,parameters.getFileExt())

createBronzeTable(parameters.getTableNamePrefix()+'_bronze',df,globalDataLakeConfig)



