# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------



# COMMAND ----------



df = createBronzeDataframe(specificTaskParameters.getFileName(),globalDataLakeConfig,specificTaskParameters.getFileExt())

createBronzeTable(specificTaskParameters.getTableName(),df,globalDataLakeConfig)



