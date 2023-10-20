# Databricks notebook source
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ###could do the cmd3 box way or do cmd5 due to metadata driven pipeline

# COMMAND ----------

#this parameter would need to be passed in via adf then converted with fromJSON function to make the structtype

schema = '{"fields":[{"metadata":{},"name":"Id","nullable":true,"type":"integer"},{"metadata":{},"name":"Pregnancies","nullable":true,"type":"integer"},{"metadata":{},"name":"Glucose","nullable":true,"type":"integer"},{"metadata":{},"name":"Systolic","nullable":true,"type":"integer"},{"metadata":{},"name":"Diastolic","nullable":true,"type":"integer"},{"metadata":{},"name":"SkinThickness","nullable":true,"type":"integer"},{"metadata":{},"name":"Insulin","nullable":true,"type":"integer"},{"metadata":{},"name":"BMI","nullable":true,"type":"double"},{"metadata":{},"name":"DiabetesPedigreeFunction","nullable":true,"type":"double"},{"metadata":{},"name":"Age","nullable":true,"type":"integer"},{"metadata":{},"name":"Outcome","nullable":true,"type":"integer"}],"type":"struct"}'
