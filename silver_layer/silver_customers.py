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
df = spark.sql("SELECT * FROM supply_chain_bronze.customers")
df.show()

# COMMAND ----------

df = replace_newlines(df, "Address", "CompanyName")
df = remove_duplicate_rows(df)
df = convert_column_types(df)
df = fill_empty_fields(df)


def split_address_column(df: DataFrame, address_column: str) -> DataFrame:
    # Split the address column into an array of parts
    split_col = split(df[address_column], ' ')
    df = df.withColumn('Street', concat_ws(' ', split_col[0], split_col[1])) \
          . withColumn('ZipCode', split_col[size(split_col) - 2]) \
          . withColumn('City', split_col[size(split_col) - 1])
    df = df.drop("Address")
    return df

df = split_address_column(df, "Address")
df.show(10)

# COMMAND ----------

# Saving Silver Layer

database = "supply_chain_silver"
location = "/mnt/demo/silver/" # add additional sub-location for this table
table = "customers"

df.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
