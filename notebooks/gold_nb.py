# Databricks notebook source
# MAGIC %md
# MAGIC date read,write and create delta tables

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

silver='abfss://silver@nyctaxisa.dfs.core.windows.net'
gold='abfss://gold@nyctaxisa.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %md
# MAGIC database creation

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists gold;
# MAGIC drop database taxi_db cascade;
# MAGIC
# MAGIC    

# COMMAND ----------

# MAGIC %md
# MAGIC data_zone

# COMMAND ----------

df_zone=spark.read.format('parquet')\
                  .load(f'{silver}/trip_zone')\
                 

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format("delta") \
    .mode("append") \
    .option("path", "abfss://gold@nyctaxisa.dfs.core.windows.net/trip_zone_delta/") \
    .saveAsTable("gold.trip_zone")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone;

# COMMAND ----------

df_type=spark.read.format('delta')\
                  .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format("delta") \
    .mode("append") \
    .option("path", "abfss://gold@nyctaxisa.dfs.core.windows.net/trip_type_delta/") \
    .saveAsTable("gold.trip_type")

# COMMAND ----------

df_data2025=spark.read.format('parquet')\
                  .load(f'{silver}/trip2025data')

# COMMAND ----------

df_data2025.write.format("delta") \
    .mode("append") \
    .option("path", "abfss://gold@nyctaxisa.dfs.core.windows.net/tripdata2025_delta/") \
    .saveAsTable("gold.tripdata2025")

# COMMAND ----------

