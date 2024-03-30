# Databricks notebook source
# MAGIC %run "/Workspace/Users/praful.rahangdale@marsh.com/Day 2/Include Variable"

# COMMAND ----------

dbfs:/mnt/cloudthats3/formula1_raw

# COMMAND ----------

# MAGIC %fs ls-/mnt/cloudthats3/formula1_raw

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/pit_stops.json")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`dbfs:/mnt/cloudthats3/formula1_raw/pit_stops.json`;

# COMMAND ----------

df=spark.read.json(f"{input_raw_files}/pit_stops.json",multiLine=True)

# COMMAND ----------

df_final=df.drop("url").withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

df_final.write.mode("overwrite").parquet(f"{output_path}praful/pit_stops")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from parquet.`dbfs:/mnt/cloudthats3/output_formula1/praful/pit_stops`
