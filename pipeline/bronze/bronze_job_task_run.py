# Databricks notebook source
# import os
# dbutils.widgets.removeAll()
# dbutils.widgets.dropdown("readFilePath",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
# dbutils.widgets.dropdown("database_folder",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
# dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
# dbutils.widgets.dropdown("database",os.getenv("database"),[f"{os.getenv('database')}"])
# dbutils.widgets.dropdown("sourceName","constructor/",["constructor/"])
# dbutils.widgets.dropdown("fileExt","json",["json"])
# dbutils.widgets.dropdown("loadType","full",["full"])
# dbutils.widgets.dropdown("destTablePrefix","constructor997",["constructor997"])

# COMMAND ----------

# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------



df = createBronzeDataframe(globalTemplateEnv.getSourceName(),globalDataLakeConfig,globalTemplateEnv.getFileExt())

createBronzeTable(globalTemplateEnv.getTableNamePrefix()+'_bronze',df,globalDataLakeConfig, globalTemplateEnv.getloadType())



