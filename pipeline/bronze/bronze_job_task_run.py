# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/schemas

# COMMAND ----------

df = createBronzeDataframe("Healthcare-Diabetes.csv",globalDataLakeConfig,"csv")

createBronzeTable("diabetestest_bronze",df,globalDataLakeConfig)



