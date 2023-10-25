# Databricks notebook source
# MAGIC %run ./silver_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

dropcols = globalTemplateEnv.getDropColumns().split(",")
identitycol = globalTemplateEnv.getIdentityColumns().split(",")

df = createSilverDataframe(globalTemplateEnv.getDestTablePrefix()+'_bronze',globalDataLakeConfig,dropCols=dropcols,identityCols=identitycol)




createSilverTable(globalTemplateEnv.getDestTablePrefix()+'_silver',df,globalDataLakeConfig,globalTemplateEnv.getloadType())

