# Databricks notebook source
# MAGIC %md
# MAGIC DATA READING
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic capabilities
# MAGIC

# COMMAND ----------

p_file_name = dbutils.widgets.text("file_name", "")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC READ STREAM
# MAGIC

# COMMAND ----------

df = spark.readStream.format("CloudFiles").\
  option("CloudFiles.format", "parquet").option("CloudFiles.schemaLocation", f"abfss://bronze@databricksgoldenlake.dfs.core.windows.net/checkpoint_{p_file_name}")\
  .load(f"abfss://source@databricksgoldenlake.dfs.core.windows.net/{p_file_name}")


display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC WRITE STREAM

# COMMAND ----------

df.writeStream.format("parquet").option("checkpointLocation", f"abfss://bronze@databricksgoldenlake.dfs.core.windows.net/checkpoint_/{p_file_name}").option("path", f"abfss://bronze@databricksgoldenlake.dfs.core.windows.net/{p_file_name}").trigger(once=True).start()

# COMMAND ----------

df = spark.read.format("parquet").load(f"abfss://source@databricksgoldenlake.dfs.core.windows.net/{p_file_name}")
df.display()