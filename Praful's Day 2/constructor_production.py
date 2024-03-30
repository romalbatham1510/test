# Databricks notebook source
# MAGIC %run "/Workspace/Users/praful.rahangdale@marsh.com/Day 2/Include Variable"

# COMMAND ----------

input_raw_files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`dbfs:/mnt/cloudthats3/formula1_raw/constructors.json`;

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/constructors.json")

# COMMAND ----------

df_final=df.drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}praful/constructor")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/praful/constructor`
