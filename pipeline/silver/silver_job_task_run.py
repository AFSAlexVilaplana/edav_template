# Databricks notebook source
# MAGIC %run ./silver_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

df = createSilverDataframe("diabetestest_bronze",globalDataLakeConfig)

createSilverTable("diabetestest_silver",df,globalDataLakeConfig)

