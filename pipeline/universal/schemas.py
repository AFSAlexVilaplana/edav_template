# Databricks notebook source
from pyspark.sql.types import *

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




