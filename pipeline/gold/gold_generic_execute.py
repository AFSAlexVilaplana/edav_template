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
# dbutils.widgets.dropdown("destTablePrefix","constructor995",["constructor995"])
# dbutils.widgets.multiselect("dropCols","url",["url","constructorRef"])
# dbutils.widgets.multiselect("identityCols","constructorId",['constructorId'])

# COMMAND ----------

# MAGIC %run ./gold_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------



df = createGoldDataframe(globalTemplateEnv.getTableNamePrefix()+'_silver',globalDataLakeConfig)


createGoldTable(globalTemplateEnv.getTableNamePrefix()+'_gold',df,globalDataLakeConfig,globalTemplateEnv.getloadType())

