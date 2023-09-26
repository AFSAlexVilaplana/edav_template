# Databricks notebook source
import requests
import json

paramList = [['csv','full','healthcare-diabetes/','diabetestest'],['json','full','constructor/','constructor']]
token = 'dapie640c593a38bdb223dd74cc3fe2de77c-3'

           

dbutils.widgets.dropdown('job_id','548390818274247',['548390818274247'])

workspace = spark.conf.get("spark.databricks.workspaceUrl")
url = f"https://{workspace}/api/2.0/jobs/run-now"
print(url)
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
    print(response.text)



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
