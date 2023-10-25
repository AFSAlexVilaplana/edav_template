# Databricks notebook source
# MAGIC %md
# MAGIC ###probably should use adf to call the specific notebooks rather than this. however, purpose of this notebook is to call in a passed parameter value called notebookPath to perform custom logic

# COMMAND ----------

# MAGIC %run ../universal/environment_setup

# COMMAND ----------

if not globalTemplateEnv.getGoldCustomNotebookPath():
    globalTemplateEnv.setGoldCustomNotebookPath("/Repos/alexander.vilaplana@accenturefederal.com/edav_template/pipeline/gold/gold_generic_execute")


params={
    "database": globalTemplateEnv.getDatabase(),
    "databaseFolder": globalTemplateEnv.getDatabaseFolder(),
    "sourceName": globalTemplateEnv.getSourceName(),
    "readFilePath": globalTemplateEnv.getReadFilePath(),
    "fileExt": globalTemplateEnv.getFileExt(),
    "destTablePrefix": globalTemplateEnv.getDestTablePrefix(),
    "loadType": globalTemplateEnv.getloadType(),
    "dropColumns": globalTemplateEnv.getDropColumns(),
    "identityColumns": globalTemplateEnv.getIdentityColumns(),
    "silverCustomNotebookPath": globalTemplateEnv.getSilverCustomNotebookPath(),
    "goldCustomNotebookPath": globalTemplateEnv.getGoldCustomNotebookPath(),
    "bronzeCustomNotebookPath": globalTemplateEnv.getBronzeCustomNotebookPath()

}

dbutils.notebook.run(globalTemplateEnv.getGoldCustomNotebookPath(),0,params)

dbutils.notebook.exit(globalTemplateEnv.getGoldCustomNotebookPath())
