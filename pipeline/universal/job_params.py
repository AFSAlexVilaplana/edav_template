# Databricks notebook source
# MAGIC %md
# MAGIC # this notebook can likely be deleted since adf is orchestrator now

# COMMAND ----------

import os

dbutils.jobs.taskValues.set(key = "database", value = dbutils.widgets.get("database"))
dbutils.jobs.taskValues.set(key = "databaseFolder", value = dbutils.widgets.get("databaseFolder"))
dbutils.jobs.taskValues.set(key = "sourceName", value = dbutils.widgets.get("sourceName"))
dbutils.jobs.taskValues.set(key = "readFilePath", value = dbutils.widgets.get("readFilePath").strip())
# dbutils.jobs.taskValues.set(key = "silverSchema", value = dbutils.widgets.get("silverSchema"))
# dbutils.jobs.taskValues.set(key = "goldSchema", value = dbutils.widgets.get("goldSchema"))
dbutils.jobs.taskValues.set(key = "fileExt", value = dbutils.widgets.get("fileExt"))
dbutils.jobs.taskValues.set(key = "destTablePrefix", value = dbutils.widgets.get("destTablePrefix"))
dbutils.jobs.taskValues.set(key = "loadType", value = dbutils.widgets.get("loadType"))
dbutils.jobs.taskValues.set(key = "dropColumns",value= dbutils.widgets.get("dropColumns"))
dbutils.jobs.taskValues.set(key = "identityColumns",value= dbutils.widgets.get("identityColumns"))
#dbutils.jobs.taskValues.set(key = "bronzeCustomNotebookPath", value = dbutils.widgets.get("bronzeCustomNotebookPath"))
dbutils.jobs.taskValues.set(key = "silverCustomNotebookPath",value= dbutils.widgets.get("silverCustomNotebookPath"))
dbutils.jobs.taskValues.set(key = "goldCustomNotebookPath",value= dbutils.widgets.get("goldCustomNotebookPath"))
dbutils.jobs.taskValues.set(key = "bucketNumber",value= dbutils.widgets.get("bucketNumber"))




