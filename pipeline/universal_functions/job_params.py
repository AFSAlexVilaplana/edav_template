# Databricks notebook source
import os

dbutils.jobs.taskValues.set(key = "database", value = os.getenv("database"))
dbutils.jobs.taskValues.set(key = "database_folder", value = os.getenv("database_folder"))
dbutils.jobs.taskValues.set(key = "scope_name", value = os.getenv("scope_name"))
dbutils.jobs.taskValues.set(key = "gold_output_database", value = os.getenv("gold_output_database"))
dbutils.jobs.taskValues.set(key = "gold_database_folder", value = os.getenv("gold_database_folder"))
