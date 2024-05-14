# Databricks notebook source
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, col, when
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType, DecimalType, CharType, ByteType, ShortType, LongType, VarcharType, LongType

import configparser

# Initializing SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Repos/piotr.wojtaszewski@unit8.co/Supply_Chain_Repo/library/lib.py
# MAGIC

# COMMAND ----------

# Retrieving data from data catalog
df = spark.sql("SELECT * FROM supply_chain_bronze.finished_goods")

# COMMAND ----------

# ETL
df = replace_newlines(df, "demandCategory")
df = remove_duplicate_rows(df)
df = convert_column_types(df)
df = fill_empty_fields(df)
df = df.withColumnRenamed("demandCategory", "Demand")
df = df.withColumnRenamed("inventory", "Product_Inventory")


# COMMAND ----------

# Saving Silver Layer

database = "supply_chain_silver"
location = "/mnt/demo/silver/" # add additional sub-location for this table
table = "finished_goods"

df.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
