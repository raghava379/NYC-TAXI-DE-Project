# Databricks notebook source
dbutils.fs.ls("abfss://bronze@nyctaxisa.dfs.core.windows.net/")


# COMMAND ----------

dbutils.fs.ls("abfss://bronze@nyctaxisa.dfs.core.windows.net/")


# COMMAND ----------

# MAGIC %md
# MAGIC data read

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Read csv data

# COMMAND ----------

# MAGIC %md
# MAGIC trip type data

# COMMAND ----------

df_trip_type=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("abfss://bronze@nyctaxisa.dfs.core.windows.net/trip_type")

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_zone=spark.read.format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("abfss://bronze@nyctaxisa.dfs.core.windows.net/trip_zone")

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC trip data

# COMMAND ----------

df_trip = spark.read.format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("abfss://bronze@nyctaxisa.dfs.core.windows.net/trip2025data/")

display(df_trip)



# COMMAND ----------

# MAGIC %md
# MAGIC transformation

# COMMAND ----------

# MAGIC %md
# MAGIC trip_type

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type=df_trip_type.withColumnRenamed("description","trip_description")
df_trip_type.display()


# COMMAND ----------

df_trip_type.write.format("delta") \
    .mode("overwrite") \
    .save("abfss://silver@nyctaxisa.dfs.core.windows.net/trip_type/")



# COMMAND ----------

dbutils.fs.ls("abfss://silver@nyctaxisa.dfs.core.windows.net/trip_type/")

# COMMAND ----------

# MAGIC %md
# MAGIC trip_zone

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

from pyspark.sql.functions import split, col, get

df_trip_zone = df_trip_zone \
    .withColumn("zone1", get(split(col("zone"), "/"), 0)) \
    .withColumn("zone2", get(split(col("zone"), "/"), 1))

display(df_trip_zone)          


# COMMAND ----------

df_trip_zone.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@nyctaxisa.dfs.core.windows.net/trip_zone/")    

# COMMAND ----------

# MAGIC %md
# MAGIC tripdata transformation 

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip=df_trip.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
               .withColumn('trip_year',year('lpep_pickup_datetime'))\
               .withColumn('trip_month',month('lpep_pickup_datetime'))\
               .withColumn('trip_day',dayofmonth('lpep_pickup_datetime'))\
               .withColumn('trip_hour',hour('lpep_pickup_datetime'))
df_trip.display()               
              

# COMMAND ----------

df_trip = spark.read.format("parquet") \
    .option("recursiveFileLookup", "true") \
    .load("abfss://bronze@nyctaxisa.dfs.core.windows.net/trip2025data/")

# COMMAND ----------

df_trip=df_trip.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
               .withColumn('trip_year',year('lpep_pickup_datetime'))\
               .withColumn('trip_month',month('lpep_pickup_datetime'))\
               .withColumn('trip_day',dayofmonth('lpep_pickup_datetime'))\
               .withColumn('trip_hour',hour('lpep_pickup_datetime'))
df_trip.display() 

# COMMAND ----------

df_trip = df_trip.select(
    "VendorID",
    "PULocationID",
    "DOLocationID",
    "total_amount",
    "payment_type"
)

display(df_trip)

# COMMAND ----------

df_trip.write.format('parquet')\
    .mode('append')\
    .save("abfss://silver@nyctaxisa.dfs.core.windows.net/trip2025data/")

# COMMAND ----------

# MAGIC %md
# MAGIC analysis

# COMMAND ----------

display(df_trip)

# COMMAND ----------

 