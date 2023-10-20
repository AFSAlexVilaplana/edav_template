# Databricks notebook source
# MAGIC %run ./gold_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------



df = createGoldDataframe(globalTemplateEnv.getDestTablePrefix()+'_silver',globalDataLakeConfig)


createGoldTable(globalTemplateEnv.getDestTablePrefix()+'_gold',df,globalDataLakeConfig,globalTemplateEnv.getloadType())

