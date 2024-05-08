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

# COMMAND ----------

start()

# COMMAND ----------

# Retrieving data from data catalog
customers_df = spark.sql("SELECT * FROM supply_chain_bronze.customers")
customer_order_df = spark.sql("SELECT * FROM supply_chain_bronze.customer_orders")
finished_goods_df = spark.sql("SELECT * FROM supply_chain_bronze.finished_goods")
manufacturing_processes_df = spark.sql("SELECT * FROM supply_chain_bronze.manufacturing_processes")
manufacturing_process_parts_df = spark.sql("SELECT * FROM supply_chain_bronze.manufacturing_process_parts")
material_master_df = spark.sql("SELECT * FROM supply_chain_bronze.material_master")

# COMMAND ----------

def replace_newlines(df: DataFrame, *columns: str) -> DataFrame:
    # Replacing newline characters with a space in the specified columns of a DataFrame.
    # For loop here is unavoidable
    for column_name in columns:
        df = df.withColumn(column_name, regexp_replace(col(column_name), "\n", " "))
    return df

def remove_duplicate_rows(df: DataFrame) -> DataFrame:
    # Removing duplicate rows from a DataFrame.
    return df.dropDuplicates()

def convert_column_types(df: DataFrame) -> DataFrame:
    # Converting column data types in a PySpark DataFrame:
    for column_name in df.columns:
        # Getting the data type of the current column
        dtype = df.schema[column_name].dataType
        
        # Checking and casting the column to the new data type if necessary
        if isinstance(dtype, (LongType, ByteType, ShortType)):
            df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
        elif isinstance(dtype, (DecimalType, DoubleType)):
            df = df.withColumn(column_name, col(column_name).cast(FloatType()))
        elif isinstance(dtype, (CharType, VarcharType)):
            df = df.withColumn(column_name, col(column_name).cast(StringType()))
    return df

def map_column_values(df: DataFrame, column_name: str, mapping: dict) -> DataFrame:
    # Determining the data type for the UDF return type based on the mapping values
    if all(isinstance(value, int) for value in mapping.values()):
        udf_return_type = IntegerType()
    else:
        udf_return_type = StringType()

    # Making UDF to apply mapping to entire column
    map_value_udf = udf(lambda value: mapping.get(value, value), udf_return_type)

    # Applying UDF
    df = df.withColumn(column_name, map_value_udf(df[column_name]))
    return df

# COMMAND ----------

customers_df = replace_newlines(customers_df, "Address", "CompanyName")
customers_df = remove_duplicate_rows(customers_df)
customers_df = convert_column_types(customers_df)

processing_time_mapping = {3:"low", 4:"medium", 5:"high"}
manufacturing_processes_df = map_column_values(manufacturing_processes_df, "processing_time", processing_time_mapping)
manufacturing_processes_df = convert_column_types(manufacturing_processes_df)
manufacturing_processes_df.show()

# COMMAND ----------

# Saving Silver Layer

database = "supply_chain_silver"
location = "/mnt/demo/silver/"
tables = ["customers", "customer_orders", "finished_goods", "manufacturing_processes", "manufacturing_process_parts", "material_master"]

count = 0
for table in tables:
    break
    df.write.format("delta").option("path",f"{location}+{tables[count]}").mode("overwrite").saveAsTable(f"{database}.{tables[count]}")
    count += 1
