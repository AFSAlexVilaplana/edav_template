# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------



df = createBronzeDataframe(globalTemplateEnv.getSourceName(),globalDataLakeConfig,globalTemplateEnv.getFileExt())



createBronzeTable(globalTemplateEnv.getDestTablePrefix()+'_bronze',df,globalDataLakeConfig, globalTemplateEnv.getloadType())



