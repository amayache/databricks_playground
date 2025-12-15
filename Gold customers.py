# Databricks notebook source
dbutils.widgets.text("init_load_flag", "0")
init_load_flag = int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

df = spark.sql ('select * from db_catalog.silver.customers')

# COMMAND ----------

# MAGIC %md
# MAGIC Removing duplicates
# MAGIC

# COMMAND ----------

df = df.dropDuplicates(subset= ["customer_id"])
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *



# COMMAND ----------

# MAGIC %md
# MAGIC surrigate key all values

# COMMAND ----------

df = df.withColumn("DimCustomerKey", monotonically_increasing_id() + lit(1))
df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dividing new vs old records

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerKey, customer_id, create_date, update_date from db_catalog.gold.DimCustomers''')
else:
    df_old = spark.sql('''select 0 DimCustomerKey, 0 customer_id, 0 create_date, 0 update_date from db_catalog.silver.customers where 1=0 ''')



# COMMAND ----------

df_old.printSchema()

# COMMAND ----------

df_old = df_old.withColumnRenamed("DimCustomerKey","DimCustomerKey_old")
df_old = df_old.withColumnRenamed("customer_id", "customer_id_old")
df_old = df_old.withColumnRenamed("create_date", "create_date_old")
df_old = df_old.withColumnRenamed("update_date", "update_date_old")


df_old.limit(10).display()


# COMMAND ----------


df_join = df.join(df_old, df_old.customer_id_old == df.customer_id, "left")
display(df_join)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Apply join

# COMMAND ----------

# MAGIC %md
# MAGIC separating new vs old records

# COMMAND ----------

df_new = df_join.filter(df_join.DimCustomerKey_old.isNull())
df_old = df_join.filter(df_join.DimCustomerKey_old.isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC preparing df old
# MAGIC

# COMMAND ----------

display(df_old)

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, current_timestamp

df_old=df_old.drop("DimCustomerKey_old")
df_old=df_old.drop("customer_id_old")
df_old=df_old.drop("update_date_old")

# renaming 'oild create date to create data
df_old = df_old.withColumnRenamed("create_date_old", "create_date")
df_old = df_old.withColumn("create_date", to_timestamp(col("create_date")))
df_old = df_old.withColumn("update_date", current_timestamp())

display(df_old)

# COMMAND ----------

df_new.display()

# COMMAND ----------



df_new=df_new.drop("DimCustomerKey")
df_new=df_new.drop("customer_id_old")
df_new=df_new.drop("update_date_old")
df_new=df_new.drop("create_date_old")


df_new = df_new.withColumnRenamed("DimCustomerKey_old","DimCustomerKey")

# renaming 'oild create date to create data

df_new = df_new.withColumn("update_date", current_timestamp())
df_new = df_new.withColumn("create_date", current_timestamp())
display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC surrogate key from 1

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", monotonically_increasing_id() + lit(1))

# COMMAND ----------

if init_load_flag == 1:
    max_surrogate_key = 0
else:
    max_surrogate_key = spark.sql(f"select max(DimCustomerKey) from db_catalog.gold.DimCustomers").collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC Union of df new and df old

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey", lit(max_surrogate_key + col("DimCustomerKey")))
df_new.display()


# COMMAND ----------

df_final=df_new.unionByName(df_old)


# COMMAND ----------

display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE 1
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
if spark.catalog.tableExists("db_catalog.gold.DimCustomers") :
    dlt_obj = DeltaTable.forPath(spark, 'abfss://gold@databricksgoldenlake.dfs.core.windows.net/DimCustomers')
    dlt_obj.alias("trg").merge(df_final.alias("src"), "trg.DimCustomerKey = src.DimCustomerKey").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
     df_final.write.mode("overwrite").format("delta")\
    .option("path","abfss://gold@databricksgoldenlake.dfs.core.windows.net/DimCustomers" ).saveAsTable("db_catalog.gold.DimCustomers")
