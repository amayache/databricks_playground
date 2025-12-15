# Databricks notebook source
datasets = [
    {
        "filename" : "orders", 
    },
    {
        "filename" : "customers", 
    },
    {
        "filename" : "products", 
    }
]

# COMMAND ----------


dbutils.jobs.taskValues.set("output_datasets", datasets)