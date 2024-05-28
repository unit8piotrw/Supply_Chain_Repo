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

# COMMAND ----------

df.show(10)
df.printSchema()

# COMMAND ----------

orders_by_customers = df.groupBy("CustomerID").agg(count("OrderID").alias("OrderCount"))
orders_by_customers.show() # Orders by customer

# COMMAND ----------


orders_by_product = df.groupBy("productID").agg(count("OrderID").alias("OrderCount")) # Order count by product
orders_by_product.show()

# COMMAND ----------

df_with_month = df.withColumn("Month", month(df["OrderDate"]))
date_distribution = df_with_month.groupBy("Month").count()

current_demand = df.withColumn("OrderYearMonth", col("OrderDate").substr(1, 7))

current_demand.show()

# COMMAND ----------

orders_by_status = df.groupBy("status").agg(count("OrderID").alias("OrderCount"))
orders_by_status.show()

# COMMAND ----------

# Saving Gold Layer

database = "supply_chain_gold" # Azure
location = "/mnt/demo/gold/"
tables = ["orders_by_customer", "orders_by_product", "orders_by_month", "orders_by_status", "current_demand"]

orders_by_customers.write.format("delta")\
    .option("path",f"{location}+{tables[0]}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{tables[0]}")

orders_by_product.write.format("delta")\
    .option("path",f"{location}+{tables[1]}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{tables[1]}")

date_distribution.write.format("delta")\
    .option("path",f"{location}+{tables[2]}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{tables[2]}")

orders_by_status.write.format("delta")\
    .option("path",f"{location}+{tables[3]}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{tables[3]}")

current_demand.write.format("delta")\
    .option("path",f"{location}+{tables[4]}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{tables[4]}")
