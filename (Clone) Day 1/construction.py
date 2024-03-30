# Databricks notebook source
df=spark.read.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")
from pyspark.sql.functions import *
df_final=df.withColumn("ingestion_date",current_timestamp()).drop("url")
df_final.write.saveAsTable("romal.constructor")

display(df_final)

# COMMAND ----------

(spark
.read\
.json("dbfs:/FileStore/tables/formula1_raw/constructors.json")
.withColumn("ingestion_date",current_timestamp())\
.drop("url")\
.write\
.mode("overwrite")\
.saveAsTable("romal.constructor2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table romal.construction3 as 
# MAGIC select *,current_timestamp() as ingestion_date  from json.`dbfs:/FileStore/tables/formula1_raw/constructors.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from romal.construction3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/FileStore/tables/formula1_raw/circuits.csv`
# MAGIC
# MAGIC df5=spark.read.option("header",True).option("inferSchema",True).querie
