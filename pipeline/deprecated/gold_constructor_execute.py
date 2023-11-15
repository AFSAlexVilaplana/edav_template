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

# MAGIC %run ../universal/functions

# COMMAND ----------

# MAGIC %run ./gold_functions

# COMMAND ----------

# MAGIC %run ../universal/environment_setup

# COMMAND ----------


df = createGoldDataframe(globalTemplateEnv.getDestTablePrefix()+'_silver',globalDataLakeConfig)

df = df.groupBy("Nationality","ingestion_date").agg(count("*").alias("countryCount"))
display(df)

# COMMAND ----------

createGoldTable(globalTemplateEnv.getDestTablePrefix()+'_gold',df,globalDataLakeConfig,globalTemplateEnv.getloadType())
