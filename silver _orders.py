# Databricks notebook source
pd = spark.read.format("parquet").load("abfss://bronze@databricksgoldenlake.dfs.core.windows.net/orders")



# COMMAND ----------

display(pd)

# COMMAND ----------

pd.printSchema()

# COMMAND ----------

pd.withColumnRenamed("_rescued_data","rescued")


display(pd.limit(10))

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

pd = pd.withColumn("order_date", to_timestamp(col('order_date')))
display(pd.limit(10))

# COMMAND ----------

from pyspark.sql.functions import year

pd = pd.withColumn("year", year(col("order_date") ))
display(pd.limit(10))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
df = pd.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))


display(df)


# COMMAND ----------

df2 = df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

display(df2)


# COMMAND ----------

df3 = df2.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))

display(df3)

# COMMAND ----------

# MAGIC %md
# MAGIC CLASS OOP

# COMMAND ----------

class windows:

  def dense_rank(self, df):
         df = df.withColumn("dense_rank", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
         return df
  def flag_rank(self, df):
        df =  df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df
  def row_rank(self, df):
        df =  df.withColumn("row_number", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df

# COMMAND ----------

df_new = pd

display(pd)

# COMMAND ----------

obj = windows()
df_new = obj.dense_rank(df_new)
df_new = obj.flag_rank(df_new)
df_new = obj.row_rank(df_new)

display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC DATA Writing

# COMMAND ----------

df_new.write.format("delta").mode("append").save("abfss://silver@databricksgoldenlake.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS db_catalog.silver.orders
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://silver@databricksgoldenlake.dfs.core.windows.net/orders"