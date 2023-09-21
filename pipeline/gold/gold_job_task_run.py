# Databricks notebook source
# MAGIC %run ./gold_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

df = createGoldDataframe(globalPersistentTaskParameters.getTableNamePrefix()+'_silver',globalDataLakeConfig)

createGoldTable(globalPersistentTaskParameters.getTableNamePrefix()+'_gold',df,globalDataLakeConfig,globalPersistentTaskParameters.getloadType())

