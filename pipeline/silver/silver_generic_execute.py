# Databricks notebook source
# import os
# dbutils.widgets.removeAll()
# dbutils.widgets.dropdown("readFilePath",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
# dbutils.widgets.dropdown("database_folder",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
# dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
# dbutils.widgets.dropdown("database",os.getenv("database"),[f"{os.getenv('database')}"])
# dbutils.widgets.dropdown("sourceName","constructor/",["constructor/"])
# dbutils.widgets.dropdown("fileExt","json",["json"])
# dbutils.widgets.dropdown("loadType","full",["full","incremental"])
# dbutils.widgets.dropdown("destTablePrefix","constructor997",["constructor997"])
# dbutils.widgets.multiselect("dropCols","url",["url","constructorRef"])
# dbutils.widgets.multiselect("identityCols","constructorId",['constructorId'])

# COMMAND ----------

# MAGIC %run ./silver_functions
# MAGIC

# COMMAND ----------

dropcols = dbutils.widgets.get("dropCols").split(",")
identitycol = [dbutils.widgets.get("identityCols")]

df = createSilverDataframe(globalTemplateEnv.getTableNamePrefix()+'_bronze',globalDataLakeConfig,dropCols=dropcols,identityCols=identitycol)




createSilverTable(globalTemplateEnv.getTableNamePrefix()+'_silver',df,globalDataLakeConfig,globalTemplateEnv.getloadType())

