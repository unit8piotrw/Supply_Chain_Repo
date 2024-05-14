# Databricks notebook source
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, col, when, to_date
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType, DecimalType, CharType, ByteType, ShortType, LongType, VarcharType, LongType, DateType, TimestampType

import configparser

# Initializing SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Repos/piotr.wojtaszewski@unit8.co/Supply_Chain_Repo/library/lib.py

# COMMAND ----------

# Retrieving data from data catalog
df = spark.sql("SELECT * FROM supply_chain_bronze.customer_orders")

# COMMAND ----------

# Transforming using a mix of ready-made functions from the lib and custom code

df = remove_duplicate_rows(df)
df = convert_column_types(df)
df = df.withColumn("max_delivery_date", to_date(df["max_delivery_date"]))
df = df.drop("Date")
df = convert_date_format(df)
df = fill_empty_fields(df)
df.show(10)

# COMMAND ----------

# Saving Silver Layer

database = "supply_chain_silver"
location = "/mnt/demo/silver/" # add additional sub-location for this table
table = "customer_orders"

df.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
