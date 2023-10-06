# Databricks notebook source
# MAGIC %md
# MAGIC ###probably should use adf to call the specific notebooks rather than this. hwoever, purpose of this notebook is to call in a passed parameter value called notebookPath to perform custom logic

# COMMAND ----------

import os
#dbutils.widgets.removeAll()
dbutils.widgets.dropdown("notebookPath","<add_path>",[f"<add_path>"])

# COMMAND ----------

params = {

}

df = dbutils.notebook.run(dbutils.widgets.get("notebookPath"),0,params)

