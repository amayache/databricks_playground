# Databricks notebook source
df=spark.read.table("db_catalog.bronze.regions")

display(df)

# COMMAND ----------

df.drop("_rescued_data")

# COMMAND ----------

df.write.format('delta').mode("append").save("abfss://silver@databricksgoldenlake.dfs.core.windows.net/regions")


# COMMAND ----------

df = spark.read.format('delta').load("abfss://silver@databricksgoldenlake.dfs.core.windows.net/regions")

display(df)