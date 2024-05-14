# Databricks notebook source
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, col, when, split, concat_ws, size, count, month

from pyspark.sql.types import IntegerType, FloatType, StringType, DoubleType, DecimalType, CharType, ByteType, ShortType, LongType, VarcharType, LongType

import configparser

# Initializing SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %run /Workspace/Repos/piotr.wojtaszewski@unit8.co/Supply_Chain_Repo/library/lib.py

# COMMAND ----------

# Retrieving data from data catalog
df = spark.sql("SELECT * FROM supply_chain_silver.demand_domain")
df_inv = spark.sql("SELECT * FROM supply_chain_silver.manufacturing_domain")

# COMMAND ----------

df.show(10)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import desc

# Getting what is currently on order
filtered_df = df.filter(df.status == "closed")
ordered_df = filtered_df.groupBy("productID").agg(count("productID").alias("ProductsOrdered"))

# Joining product_count_df with df_inv to get the Product_Inventory column
joined_df = df_inv.join(ordered_df, "productID", "left")

joined_df = joined_df.select("productID", "ProductsOrdered", "Product_Inventory")
joined_df = joined_df.withColumn("Difference", col("Product_Inventory") - col("ProductsOrdered"))

result_df = joined_df.select("productID", "ProductsOrdered", "Product_Inventory", "Difference")
result_df = result_df.dropDuplicates()
result_df.orderBy(desc("productID")).show(100)


# COMMAND ----------

# Saving Gold Layer

database = "supply_chain_gold" # Azure
location = "/mnt/demo/gold/"
table = "missing_inventory"

result_df.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
