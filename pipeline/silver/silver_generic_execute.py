# Databricks notebook source
# MAGIC %run ./silver_functions
# MAGIC

# COMMAND ----------

dropcols = globalTemplateEnv.getDropColumns()
identitycol = globalTemplateEnv.getIdentityColumns()

df = createSilverDataframe(globalTemplateEnv.getDestTablePrefix()+'_bronze',globalDataLakeConfig,dropCols=dropcols,identityCols=identitycol)




createSilverTable(globalTemplateEnv.getDestTablePrefix()+'_silver',df,globalDataLakeConfig,globalTemplateEnv.getloadType())

