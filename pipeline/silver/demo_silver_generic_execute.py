# Databricks notebook source
# MAGIC %run ./silver_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

# DBTITLE 1,read from bronze
#run create silver dataframe notebook with default parameters
df = createSilverDataframe(globalTemplateEnv.getDestTablePrefix()+'_bronze',globalDataLakeConfig)

display(df)


# COMMAND ----------

# DBTITLE 1,drop unused columns
#pull dropCols parameter from config. if dropCols list is empty then function is skipped
dropCols = globalTemplateEnv.getDropColumns().split(",") #['']

df = df.drop(*dropCols)

display(df)

# COMMAND ----------

# DBTITLE 1,Drop duplicates from dataframe
#pull identityCols parameter from config. if identityCols list is empty then function is skipped
identityCols = globalTemplateEnv.getIdentityColumns().split(",")

df = df.drop_duplicates(identityCols)

print(df.count())

# COMMAND ----------

# DBTITLE 1,write to silver table
createSilverTable(globalTemplateEnv.getDestTablePrefix()+'_silver',df,globalDataLakeConfig,globalTemplateEnv.getloadType())

# COMMAND ----------

display(spark.sql(f"select * from  {globalTemplateEnv.getDatabase()}.{globalTemplateEnv.getDestTablePrefix()+'_silver'}"))
