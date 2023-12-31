# Databricks notebook source
# MAGIC %run ../universal/functions

# COMMAND ----------

dropcols = globalTemplateEnv.getDropColumns().split(",")
identitycol = globalTemplateEnv.getIdentityColumns().split(",")

# COMMAND ----------

import logging
import sys

logger = logging.getLogger()

# Raw Read
# setSchema
df = createDataframe(
    medallionStep = 'bronze',
    source = globalTemplateEnv.getDestTablePrefix(),
    dataLakeConfig = globalDataLakeConfig,
    fileFormat = globalTemplateEnv.getFileExt()
    #, schema = setSchema
)

# COMMAND ----------

globalTemplateEnv.getDestTablePrefix()

# COMMAND ----------

display(df)

# COMMAND ----------

# Transform
    # Try: Logic to run
    # Except:  Here you can handle the error
    # Else: If there is no exception then this block will be executed
    # Finally: Finally block always gets executed either exception is generated or not

try:
    # Run Generic Logic
    # perform_operation()
    logger.info("Operation succeeded")
    logger.error("this is an error")
except (Exception, ValueError) as e:
    # Handle Exception
    logging.error(f"This is my error: {e}")
    logger.info("Retry") 
    dbutils.notebook.exit(f"Error occurred: {e}", "ERROR")
else:
    logger.info(f"Fallback - Alternative logic") 
finally:
    logger.info('This is always executed') 


# COMMAND ----------

# Validation Step

# assert 1
# assert 2
#   
#   

# COMMAND ----------

globalTemplateEnv.getDestTablePrefix()

# COMMAND ----------



# COMMAND ----------

createTable(
    loadType = globalTemplateEnv.getloadType(),
    dataLakeConfig = globalDataLakeConfig, 
    df = df,
    tableName = globalTemplateEnv.getDestTablePrefix(),
    medallionStep = 'silver'
    )
