# Databricks notebook source
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, col, when, split, concat_ws, size
from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType, DecimalType, CharType, ByteType, ShortType, LongType, VarcharType, LongType

import configparser

# Initializing SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Repos/piotr.wojtaszewski@unit8.co/Supply_Chain_Repo/library/lib.py

# COMMAND ----------

# Retrieving data from data catalog
finished_goods_df = spark.sql("SELECT * FROM supply_chain_silver.finished_goods")
manufacturing_processes_df = spark.sql("SELECT * FROM supply_chain_silver.manufacturing_processes")
manufacturing_process_parts_df = spark.sql("SELECT * FROM supply_chain_silver.manufacturing_process_parts")
material_master_df = spark.sql("SELECT * FROM supply_chain_silver.material_master")

# COMMAND ----------

df = finished_goods_df.join(manufacturing_processes_df, "productID", "left")
df = manufacturing_process_parts_df.join(df, on='ProcessID', how='right')
df = df.join(material_master_df, on="MaterialID", how="left") # Some materials are dropped here because they are never used maybe it would make sense to limit meterial number to 20-30 instead of 100
df = df.drop("Demand")
df.show(10)

# COMMAND ----------

# Saving Silver Layer

database = "supply_chain_silver"
location = "/mnt/demo/silver/" # add additional sub-location for this table
table = "manufacturing_domain"

df.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
