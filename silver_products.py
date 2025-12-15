# Databricks notebook source
from pyspark.sql.functions import *


df = spark.read.format("parquet").load("abfss://bronze@databricksgoldenlake.dfs.core.windows.net/products")

display(df)

# COMMAND ----------

df = df.drop("_rescued_data")


# COMMAND ----------

# MAGIC %md
# MAGIC Functions

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION db_catalog.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC RETURN p_price * 0.95
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id, price, db_catalog.bronze.discount_func(price) as discounted_price from products

# COMMAND ----------

df = df.withColumn("discount_price", expr("db_catalog.bronze.discount_func(price)"))
display(df)

# COMMAND ----------

# Define and register the UDF in Python
def upper_func(p_brand):
    return p_brand.upper() if p_brand is not None else None

spark.udf.register(
    "upper_func",
    upper_func,
    "string"
)

# COMMAND ----------

from pyspark.sql.functions import expr

df = df.withColumn("brand", expr("upper_func(brand)"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('delta').mode('append').save("abfss://silver@databricksgoldenlake.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS db_catalog.silver.products
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricksgoldenlake.dfs.core.windows.net/products"