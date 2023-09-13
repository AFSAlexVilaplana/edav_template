# Databricks notebook source
# MAGIC %run ./bronze_functions

# COMMAND ----------

df = create_bronze_dataframe("Healthcare-Diabetes.csv",globalDataLakeConfig)
create_bronze_table("diabetesTest",df,globalDataLakeConfig)


# COMMAND ----------

df = spark.sql("select count(*) from s3t8.diabetestest")
display(df)
