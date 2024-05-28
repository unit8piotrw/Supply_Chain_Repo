# Databricks notebook source
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, col, when, split, concat_ws, size, count, month, desc, avg

from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType, DecimalType, CharType, ByteType, ShortType, LongType, VarcharType, LongType

import configparser

# Initializing SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Repos/piotr.wojtaszewski@unit8.co/Supply_Chain_Repo/library/lib.py

# COMMAND ----------

# Retrieving data from data catalog
df = spark.sql("SELECT * FROM supply_chain_silver.manufacturing_domain")

# COMMAND ----------

df.show(100)
df.printSchema()

# COMMAND ----------



grouped_data = df.groupBy("productID")

sum_by_category = grouped_data.sum("ProcessingTime").withColumnRenamed("sum(ProcessingTime)", "TotalProcessingTime")
sum_by_category.orderBy(desc("TotalProcessingTime")).show(100)


# COMMAND ----------

avg_processing_time_by_facility_df = df.groupBy("FacilityID").agg(avg("ProcessingTime").alias("AvgProcessingTime"))
avg_processing_time_by_facility_df.orderBy(desc("AvgProcessingTime")).show(100)

# COMMAND ----------


inventory_levels_df = df.select("MaterialID", "Mat_SKU", "Mat_Inv").dropDuplicates()
inventory_levels_df.show()
print(inventory_levels_df.count())

# COMMAND ----------

# Saving Gold Layer

database = "supply_chain_gold" # Azure
location = "/mnt/demo/gold/"
tables = ["inventory_table"]

df.write.format("delta")\
    .option("path",f"{location}+{tables[0]}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{tables[0]}")
