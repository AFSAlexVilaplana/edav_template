# Databricks notebook source
import os
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("readFilePath",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
dbutils.widgets.dropdown("database_folder",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
dbutils.widgets.dropdown("database",os.getenv("database"),[f"{os.getenv('database')}"])
dbutils.widgets.dropdown("sourceName","constructor/",["constructor/"])
dbutils.widgets.dropdown("fileExt","json",["json"])
dbutils.widgets.dropdown("loadType","full",["full"])
dbutils.widgets.dropdown("destTablePrefix","constructor999",["constructor999"])

# COMMAND ----------

# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------



df = createBronzeDataframe(globalPersistentTaskParameters.getSourceName(),globalDataLakeConfig,globalPersistentTaskParameters.getFileExt())

createBronzeTable(globalPersistentTaskParameters.getTableNamePrefix()+'_bronze',df,globalDataLakeConfig, globalPersistentTaskParameters.getloadType())



