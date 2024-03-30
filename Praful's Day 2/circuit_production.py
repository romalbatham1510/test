# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run "/Workspace/Users/praful.rahangdale@marsh.com/Day 2/Include Variable"
# MAGIC

# COMMAND ----------

df=spark.read.csv(f"{input_raw_files}/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

df_final=df.drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}praful/circuit")
