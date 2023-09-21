# Databricks notebook source
# MAGIC %run ./silver_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------



df = createSilverDataframe(globalPersistentTaskParameters.getTableNamePrefix()+'_bronze',globalDataLakeConfig)

createSilverTable(globalPersistentTaskParameters.getTableNamePrefix()+'_silver',df,globalDataLakeConfig,globalPersistentTaskParameters.getloadType())

