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
df_demand = spark.sql("SELECT * FROM supply_chain_gold.current_demand")
df_inv = spark.sql("SELECT * FROM supply_chain_gold.missing_inventory")
df_manufacturing = spark.sql("SELECT * FROM supply_chain_gold.inventory_table")
df_manufacturing = df_manufacturing.select("MaterialID", "ProcessPartID", "Req_Prod_Quant", "productID", "FacilityID", "ProcessingTime")


# COMMAND ----------

df_inv.printSchema()
df_inv.show(10)

# COMMAND ----------

df_demand.printSchema()
df_demand.show(10)

# COMMAND ----------

# Filter the dataframe where status is "open"
df_open = df_demand.filter(col("status") == "open")
#df_open = df_open.filter(col("ProductID") == '5')
df_open.show(100)

# Group by ProductID and OrderYearMonth and count the number of rows in each group
orders_df = df_open.groupBy("ProductID", "OrderDate").count().orderBy("productID")
orders_df = orders_df.withColumnRenamed("OrderDate", "OrderYearMonth")
orders_df.show(100)


# COMMAND ----------

from pyspark.sql.functions import sum as _sum, lag
from pyspark.sql.window import Window

# Sort orders_df by ProductID and OrderYearMonth in ascending order
orders_df = orders_df.orderBy("ProductID", "OrderYearMonth")

# Join orders_df with inv_df on ProductID
df_joined = orders_df.join(df_inv.select("ProductID", "Product_Inventory"), on="ProductID", how="left")

# Define a window partitioned by ProductID, ordered by OrderYearMonth
window = Window.partitionBy('ProductID').orderBy('OrderYearMonth')

# Calculate running total of count
df_joined = df_joined.withColumn('running_total', _sum('count').over(window))

# Subtract running total from initial inventory to get remaining inventory
df_joined = df_joined.withColumn('end_inventory', col('Product_Inventory') - col('running_total'))
df_joined = df_joined.withColumn('start_inventory', lag('end_inventory').over(window))
df_joined = df_joined.withColumn('start_inventory', when(col("start_inventory").isNull(), col("Product_Inventory")).otherwise(col("start_inventory")))


df_joined = df_joined.withColumn('Total_produced', when(col("end_inventory") < 0, col("end_inventory") * -1).otherwise(0))
df_joined = df_joined.withColumn('end_inventory', when(col("end_inventory") < 0, 0).otherwise(col("end_inventory")))
df_joined = df_joined.withColumn('start_inventory', lag('end_inventory').over(window))
df_joined = df_joined.withColumn('start_inventory', when(col("start_inventory").isNull(), col("Product_Inventory")).otherwise(col("start_inventory")))

df_joined = df_joined.withColumn('Total_produced_before', lag('Total_produced').over(window))
df_joined = df_joined.withColumn('Delta_produced', col("Total_produced") - col("Total_produced_before"))

df_joined = df_joined.withColumn('Delta_produced', when(col("Delta_produced").isNull(), col("Total_produced")).otherwise(col("Delta_produced")))

# Filter out rows where remaining inventory is greater than or equal to 0
df_final = df_joined.filter(col('Delta_produced') > 0)
df_final = df_final.select("ProductID", "OrderYearMonth", "Delta_produced")
df_final.show(100)

#df_joined.filter(col("ProductID") == "21").show(100)

# COMMAND ----------

df_final = df_final.join(df_manufacturing, "productID", "left")
df_final.show(100)

# COMMAND ----------

# Saving Gold Layer

database = "supply_chain_gold" # Azure
location = "/mnt/demo/gold/"
table = "manufacturing_job_list"

df_final.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
