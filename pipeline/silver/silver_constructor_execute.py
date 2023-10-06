# Databricks notebook source
# MAGIC %md
# MAGIC ###The idea with this notebook is at run time you pass this path in as a parameter to the gold_controller notebook and it will execute at run time.
# MAGIC ###You can perform whatever transformations necessary on this notebook
# MAGIC
# MAGIC
# MAGIC this is an example notebook to show how you can utilize transformation specific notebooks and easily integrate to whatever table being loaded via configs passed at run time
# MAGIC
# MAGIC if there are generic transformations that need to be done at the gold or silver level it is probably best to put those in pipeline/universal/functions and then import that notebook in here and pass the dataframe through
# MAGIC

# COMMAND ----------

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

# MAGIC %run ./silver_functions

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

dropcols = dbutils.widgets.get("dropCols").split(",")
identitycol = [dbutils.widgets.get("identityCols")]

#dropcols and identitycol not added to below function because theyre not required. see the function work with these two arguments in the silver_generic_execute notebook

df = createSilverDataframe(globalTemplateEnv.getTableNamePrefix()+'_bronze',globalDataLakeConfig)

#add constructor specific functionality here from the universal/functions notebook. or just whatever you come up with.




# COMMAND ----------

createSilverTable(globalTemplateEnv.getTableNamePrefix()+'_silver',df,globalDataLakeConfig,globalTemplateEnv.getloadType())
