# Databricks notebook source
# MAGIC %fs ls "dbfs:/FileStore/tables/formula1_raw"

# COMMAND ----------

df=spark.read.csv('dbfs:/FileStore/tables/formula1_raw/circuits.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv",header=True,inferSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### test markdown ####

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC STEP 1: Extract csv into dataframe

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/formula1_raw/circuits.csv")

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

df.select("circuitId","circuitRef").display()

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df.select(col("circuitId").alias("circuit_id")).display()

# COMMAND ----------

df.select("circuitId",col("circuitRef"),df.name,df["location"]).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.select(concat("location","country")).display()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df.select(concat("location",lit(" & "),"country").alias("loc & country")).display()

# COMMAND ----------

df.withColumnRenamed("circuitId","circuit_id").withColumnRenamed("circuitRef","circuit_ref").display()

# COMMAND ----------

df.columns

# COMMAND ----------

new_columns=['circuitId',
 'circuitRef',
 'name',
 'location',
 'country',
 'latitude',
 'longitude',
 'alt',
 'url']

# COMMAND ----------

df1=df.toDF(*new_columns)

# COMMAND ----------

df1.display()

# COMMAND ----------

help(df1.drop)

# COMMAND ----------

df2=df1.drop("url")

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.display()

# COMMAND ----------

from pyspark.sql.functions import current_date,upper;
df2.withColumn("current_time", current_date()).display();

df2.withColumn("name",upper("name")).display()

# COMMAND ----------

dbutils.fs.mkdirs ("dbfs:/FileStore/tables/output/romal/circuit")

# COMMAND ----------

# MAGIC %fs ls "dbfs:/FileStore/tables/output/romal/circuit_par"

# COMMAND ----------

df1.write.mode("overwrite").parquet("dbfs:/FileStore/tables/output/romal/circuit")

# COMMAND ----------

df3= spark.read.parquet("dbfs:/FileStore/tables/output/romal/circuit");

df1.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema romal

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.write.saveAsTable("romal.circuit_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select location,count(location) as count from romal.circuit group by location
# MAGIC union
# MAGIC select location,count(location) as count from romal.circuit_1 group by location
