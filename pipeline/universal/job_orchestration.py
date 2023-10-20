# Databricks notebook source
import os
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("readFilePath",os.getenv("read_file_path").strip(),[f"{os.getenv('read_file_path').strip()}"])
dbutils.widgets.dropdown("databaseFolder",os.getenv("database_folder").strip(),[f"{os.getenv('database_folder').strip()}"])
#dbutils.widgets.dropdown("scope",os.getenv("scope_name"),[f"{os.getenv('scope_name')}"])
dbutils.widgets.dropdown("database",os.getenv("database"),[f"{os.getenv('database')}"])
dbutils.widgets.dropdown("sourceName","constructor/",["constructor/"])
dbutils.widgets.dropdown("fileExt","json",["json"])
dbutils.widgets.dropdown("loadType","full",["full","incremental"])
dbutils.widgets.dropdown("destTablePrefix","constructor995",["constructor995"])
dbutils.widgets.multiselect("dropColumns","url",["url","constructorRef"])
dbutils.widgets.multiselect("identityColumns","constructorId",['constructorId'])
dbutils.widgets.dropdown("silverCustomNotebookPath","/Repos/alexander.vilaplana@accenturefederal.com/edav_template/pipeline/silver/silver_constructor_execute",["/Repos/alexander.vilaplana@accenturefederal.com/edav_template/pipeline/silver/silver_constructor_execute"])
dbutils.widgets.dropdown("goldCustomNotebookPath","/Repos/alexander.vilaplana@accenturefederal.com/edav_template/pipeline/gold/gold_constructor_execute",["/Repos/alexander.vilaplana@accenturefederal.com/edav_template/pipeline/gold/gold_constructor_execute"])
dbutils.widgets.dropdown("bronzeCustomNotebookPath","",[""])

# COMMAND ----------

import requests
import json
from random import randrange
import os

n = randrange(1,100)


token = dbutils.secrets.get(scope="cmod_s3t8_kv_eus",key='dbx-pat')

           

dbutils.widgets.dropdown('job_id','548390818274247',['548390818274247'])

workspace = spark.conf.get("spark.databricks.workspaceUrl")
url = f"https://{workspace}/api/2.0/jobs/run-now"

job_id = dbutils.widgets.get("job_id")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# for params in paramList:
parameters = {
    "database": dbutils.widgets.get("database"),
    "databaseFolder": dbutils.widgets.get("databaseFolder"),
    "sourceName": dbutils.widgets.get("sourceName"),
    "readFilePath": dbutils.widgets.get("readFilePath").strip(),
    "fileExt": dbutils.widgets.get("fileExt"),
    "destTablePrefix": dbutils.widgets.get("destTablePrefix"),
    "loadType": dbutils.widgets.get("loadType"),
    "dropColumns": dbutils.widgets.get("dropColumns"),
    "identityColumns": dbutils.widgets.get("identityColumns"),
    "silverCustomNotebookPath": dbutils.widgets.get("silverCustomNotebookPath"),
    "goldCustomNotebookPath": dbutils.widgets.get("goldCustomNotebookPath"),
    "bronzeCustomNotebookPath": dbutils.widgets.get("bronzeCustomNotebookPath")
    }
payload = {
    "job_id": job_id,
    "existing_cluster_id": cluster_id,
    "notebook_params": parameters
    
}
response = requests.post(url,headers={"Authorization":f"Bearer {token}"},json=payload)
print(response.text,parameters.get('dest_table_prefix'))



# COMMAND ----------

###########################################SUBPROCESS VARIATION BELOW, NOT WORKING STILL###################################


# import subprocess

# paramList = [['csv','full','healthcare-diabetes/','diabetestest']]

# #dbutils.widgets.dropdown('job_id','548390818274247',['548390818274247'])

# job_id = dbutils.widgets.get("job_id").strip()
# cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# for params in paramList:
#     parameters = {
#         "file_ext":params[0],
#         "load_type":params[1],
#         "source_name":params[2],
#         "dest_table_prefix":params[3]
#         }
#     command = f'databricks jobs run-now --job-id "{job_id}"'
#     print(command)
#     subprocess.Popen(command,shell=True)
