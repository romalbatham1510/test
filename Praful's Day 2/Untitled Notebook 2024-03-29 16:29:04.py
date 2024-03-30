# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/cloudthats3/raw

# COMMAND ----------

# MAGIC %run "/Workspace/Users/praful.rahangdale@marsh.com/Day 2/Include Variable"

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/cloudthats3/raw/Baby_Names.csv",header=True,inferSchema=True)

# COMMAND ----------

df.groupBy("Year").count().display()

# COMMAND ----------

df.write.mode("overwrite").parquet(f"{output_path}praful/Baby_Names")

# COMMAND ----------

df.write.partitionBy("Year").parquet("dbfs:/mnt/cloudthats3/raw/Baby_Names_By_Year")

# COMMAND ----------

df.write.partitionBy("Year","Sex").parquet("dbfs:/mnt/cloudthats3/raw/Baby_Names_By_Gender")

# COMMAND ----------

df.display()

# COMMAND ----------


