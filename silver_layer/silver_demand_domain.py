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
customers_df = spark.sql("SELECT * FROM supply_chain_silver.customers")
customer_orders_df = spark.sql("SELECT * FROM supply_chain_silver.customer_orders")

# COMMAND ----------

df = customer_orders_df.join(customers_df, "CustomerID", "left")
df = df.withColumnRenamed("demandCategory", "Demand")
df = df.drop("Demand")
df.show()

# COMMAND ----------

# Saving Silver Layer

database = "supply_chain_silver"
location = "/mnt/demo/silver/" # add additional sub-location for this table
table = "demand_domain"

df.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
