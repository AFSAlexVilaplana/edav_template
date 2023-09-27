# Databricks notebook source
import requests
import json
from random import randrange
import os

n = randrange(1,100)

paramList = [['csv','incremental','healthcare-diabetes/',f'diabetestest{n}'],['json','incremental','constructor/',f'constructor{n}']]
token = dbutils.secrets.get(scope="cmod_s3t8_kv_eus",key='dbx-pat')

           

dbutils.widgets.dropdown('job_id','548390818274247',['548390818274247'])

workspace = spark.conf.get("spark.databricks.workspaceUrl")
url = f"https://{workspace}/api/2.0/jobs/run-now"

job_id = dbutils.widgets.get("job_id")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

for params in paramList:
    parameters = {
        "file_ext":params[0],
        "load_type":params[1],
        "source_name":params[2],
        "dest_table_prefix":params[3]
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
