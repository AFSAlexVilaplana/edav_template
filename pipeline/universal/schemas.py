# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###could do the cmd3 box way or do cmd4 due to metadata driven pipeline

# COMMAND ----------

schema_diabetes_bronze = StructType([
    StructField("id",IntegerType(),False),
    StructField("Pregnancies",IntegerType(),False),
    StructField("Glucose",IntegerType(),False),
    StructField("Systolic",IntegerType(),False),
    StructField("Diastolic",IntegerType(),False),
    StructField("SkinThickness",IntegerType(),False),
    StructField("Insulin",IntegerType(),False),
    StructField("BMI",DoubleType(),False),
    StructField("DiabetesPedigreeFunction",DoubleType(),False),
    StructField("Age",IntegerType(),False),
    StructField("Outcome",IntegerType(),False)
]
)

schema_diabetes_silver_drop_cols = ["Pregnancies","SkinThickness"]
diabetes_silver_identity_cols = ["Id"]





# COMMAND ----------

#this parameter would need to be passed in via adf then converted with fromJSON function to make the structtype

schema = '{"fields":[{"metadata":{},"name":"Id","nullable":true,"type":"integer"},{"metadata":{},"name":"Pregnancies","nullable":true,"type":"integer"},{"metadata":{},"name":"Glucose","nullable":true,"type":"integer"},{"metadata":{},"name":"Systolic","nullable":true,"type":"integer"},{"metadata":{},"name":"Diastolic","nullable":true,"type":"integer"},{"metadata":{},"name":"SkinThickness","nullable":true,"type":"integer"},{"metadata":{},"name":"Insulin","nullable":true,"type":"integer"},{"metadata":{},"name":"BMI","nullable":true,"type":"double"},{"metadata":{},"name":"DiabetesPedigreeFunction","nullable":true,"type":"double"},{"metadata":{},"name":"Age","nullable":true,"type":"integer"},{"metadata":{},"name":"Outcome","nullable":true,"type":"integer"}],"type":"struct"}'
