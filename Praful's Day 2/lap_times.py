# Databricks notebook source
schema 
when
1. if you have large data
2. do not have header


which
1.str/list
2. pyspark datatype

# COMMAND ----------

lap_schema="raceId int, driverID int, lap int, position int, time string, millisecond int"

# COMMAND ----------

# MAGIC %run "/Workspace/Users/praful.rahangdale@marsh.com/Day 2/Include Variable"

# COMMAND ----------

df=spark.read.schema(lap_schema).csv(f"{input_raw_files}/lap_times/")

# COMMAND ----------

df.write.mode("overwrite").parquet(f"{output_path}praful/lap_times")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/output_formula1/praful/lap_times

# COMMAND ----------

display(df)
