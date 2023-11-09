# Databricks notebook source
# MAGIC %run ./bronze_functions
# MAGIC

# COMMAND ----------

# MAGIC %run ../universal/functions

# COMMAND ----------

import logging
import sys

logger = logging.getLogger()

# Raw Read

# setSchema = 

df = createDataframe(
    fileName = globalTemplateEnv.getSourceName(),
    dataLakeConfig = globalDataLakeConfig,
    fileFormat = globalTemplateEnv.getFileExt()
    #, schema = setSchema
)

# COMMAND ----------

# Transform
    # Try: This block will test the excepted error to occur
    # Except:  Here you can handle the error
    # Else: If there is no exception then this block will be executed
    # Finally: Finally block always gets executed either exception is generated or not

try:
    # Run Generic Logic
    # perform_operation()
    logger.info("Operation succeeded")

except (Exception, ValueError) as e:
    # Handle Exception
    logging.error(f"This is my error: {e}")
    logger.info("Retry") 
    # Let's Retry 3x
    for retry in range(3):
        logger.warning(f"Retrying operation (attempt {retry + 1})")
        try:
            # perform_operation()
            logger.info("It worked!")
        except (Exception, ValueError) as e:
            logger.error(f"Error occurred during retry: {e}")
            # perform_operation() failed 4 times - log an Error and Stop Notebook
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

createTable(
    load_type = globalTemplateEnv.getloadType(),
    dataLakeConfig = globalDataLakeConfig, 
    df = df,
    tableName = globalTemplateEnv.destTablePrefix,
    medallion_step = 'bronze'
    )
