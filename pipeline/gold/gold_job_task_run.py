# Databricks notebook source
# MAGIC %run ./gold_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

df = createGoldDataframe("diabetestest_silver",globalDataLakeConfig)

createGoldTable("diabetestest_gold",df,globalDataLakeConfig)

