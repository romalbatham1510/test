# Databricks notebook source
# MAGIC %run "/Workspace/Users/praful.rahangdale@marsh.com/Day 2/Include Variable"

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/cloudthats3/raw_json/adobe_sample_json.json",multiLine=True)

# COMMAND ----------

(df.withColumn("topping",explode("topping"))
.withColumn("topping_id",col("topping.id"))
.withColumn("topping_type",col("topping.type"))
.drop("topping")
.display())

# COMMAND ----------

(df.withColumn("topping",explode("topping"))
.withColumn("topping_id",col("topping.id"))
.withColumn("topping_type",col("topping.type"))
.drop("topping")
.withColumn("batters",explode("batters.batter"))
.withColumn("batters_id",col("batters.id"))
.withColumn("batters_type",col("batters.type"))
.drop("batters")
.display())

# COMMAND ----------

final_df=(df.withColumn("topping",explode("topping"))
.withColumn("topping_id",col("topping.id"))
.withColumn("topping_type",col("topping.type"))
.drop("topping")
.withColumn("batters",explode("batters.batter"))
.withColumn("batters_id",col("batters.id"))
.withColumn("batters_type",col("batters.type"))
.drop("batters"))
#.display())

# COMMAND ----------

final_df.write.saveAsTable(praful.adobe_sample_json)

# COMMAND ----------

final_df.display()

# COMMAND ----------

final_df.write.saveAsTable(naval.adobe_sample_json)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.adobe_sample where topping_id=5001

# COMMAND ----------

from pyspark.sql.functions import *
final_df.where(col("topping_id")==5001).display()

# COMMAND ----------

df.orderBy(desc("batters_type"),desc("topping_id")).display()
