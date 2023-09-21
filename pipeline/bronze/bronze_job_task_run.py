# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------



df = createBronzeDataframe(globalPersistentTaskParameters.getSourceName(),globalDataLakeConfig,globalPersistentTaskParameters.getFileExt(),schema_diabetes_bronze)

createBronzeTable(globalPersistentTaskParameters.getTableNamePrefix()+'_bronze',df,globalDataLakeConfig, globalPersistentTaskParameters.getloadType())



