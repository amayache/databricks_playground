# Databricks notebook source
df = spark.read.format("parquet").load("abfss://bronze@databricksgoldenlake.dfs.core.windows.net/customers")

display(df)

# COMMAND ----------


from pyspark.sql.functions import split, col
df = df.withColumn("domains", split(col("email"),"@")[1])
display(df)


# COMMAND ----------

from pyspark.sql.functions import count
df2 = df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers", ascending=False)

display(df2)

# COMMAND ----------

df2.filter(col("domains") == "gmail.com").display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

from pyspark import sql
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, concat

df2 = df2.withColumn("rank", dense_rank().over(Window.orderBy(col("total_customers").desc())))
display(
    df2.filter(col("rank") <= 3)
)


# COMMAND ----------

from pyspark.sql.functions import concat, col, lit
df = df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
df = df.drop("_rescued_data")
display(df)

# COMMAND ----------

df.write.format('delta').save("abfss://silver@databricksgoldenlake.dfs.core.windows.net/customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS db_catalog.silver.customers
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricksgoldenlake.dfs.core.windows.net/customers"