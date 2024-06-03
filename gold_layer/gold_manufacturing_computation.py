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
df_jobs = spark.sql("SELECT * FROM supply_chain_gold.manufacturing_job_list")
df_manufacturing = spark.sql("SELECT * FROM supply_chain_gold.inventory_table")


# COMMAND ----------

df_jobs.printSchema()
df_jobs.show(10)

# A day has 24h of processing time avaliable
# Each Facility has 1 production spot

# COMMAND ----------

from pyspark.sql.functions import lag, greatest, sum, to_date, add_months, date_diff, current_date, expr, lit, substring
from pyspark.sql import Window

df_jobs = spark.sql("SELECT * FROM supply_chain_gold.manufacturing_job_list")


def schedule_jobs(df_jobs, max_hours_day):
    # Define window
    df_jobs = df_jobs.withColumn("OrderDeadline", add_months(df_jobs["OrderYearMonth"], 3))
    df_jobs = df_jobs.withColumn("CurrentDate", current_date())
    df_jobs = df_jobs.withColumn("DiffHours", date_diff(df_jobs["OrderDeadline"], df_jobs["CurrentDate"]) * max_hours_day)
    window = Window.partitionBy("FacilityID").orderBy("OrderYearMonth", "ProductID")

    # Adding the cost of switching the product category being produced
    # Calculating ProductCategories
    df_jobs = df_jobs.withColumn("ProductCategory", substring(df_jobs.ProcessPartID, -1, 1))
    df_jobs = df_jobs.withColumn('PreviousProductCat', lag('ProductCategory').over(window))
    df_jobs = df_jobs.withColumn('PreviousProductCat', when(col("PreviousProductCat").isNull(), col("ProductCategory")).otherwise(col("PreviousProductCat")))

    # Adding the cost to the processing times
    df_jobs = df_jobs.withColumn("TotalProcessingTime", col("ProcessingTime") * col("Delta_produced"))
    df_jobs = df_jobs.withColumn('TotalProcessingTime', when(col("PreviousProductCat") == col("ProductCategory"), col("TotalProcessingTime")).otherwise(col("TotalProcessingTime")+1))

    # Calculating total processing time and expected delays in each facility
    df_jobs = df_jobs.withColumn('Running_Total', sum('TotalProcessingTime').over(window))
    alerts = df_jobs.filter(col("Running_Total") > col("DiffHours"))
    alerts = alerts.withColumn("ExpectedDelay",col("Running_Total") - col("DiffHours"))

    # Calculating the minimum and maximum values of the ExpectedDelay column
    min_delay = alerts.agg({"ExpectedDelay": "min"}).collect()[0][0]
    max_delay = alerts.agg({"ExpectedDelay": "max"}).collect()[0][0]

    # Calculating the breakpoints for the delay categories
    range_20 = min_delay + 0.2 * (max_delay - min_delay)
    range_40 = min_delay + 0.4 * (max_delay - min_delay)
    range_60 = min_delay + 0.6 * (max_delay - min_delay)
    range_80 = min_delay + 0.8 * (max_delay - min_delay)

    alerts = alerts.withColumn(
        "DelayCategory",
        when(col("ExpectedDelay") <= range_20, lit("Category_1"))
        .when((col("ExpectedDelay") > range_20) & (col("ExpectedDelay") <= range_40), lit("Category_2"))
        .when((col("ExpectedDelay") > range_40) & (col("ExpectedDelay") <= range_60), lit("Category_3"))
        .when((col("ExpectedDelay") > range_60) & (col("ExpectedDelay") <= range_80), lit("Category_4"))
        .otherwise(lit("Category_5"))
    )

    alerts = alerts.select("ProductID", "OrderYearMonth", "Delta_produced", "FacilityID", "OrderDeadline", "ExpectedDelay", "DelayCategory")
    alerts = alerts.dropDuplicates()
    alerts = alerts.orderBy("FacilityID", "OrderYearMonth")
    return df_jobs, alerts

df_jobs, alerts = schedule_jobs(df_jobs, max_hours_day=8)

df_jobs.show(100)

alerts.show(100)

# COMMAND ----------

# Saving Gold Layer

database = "supply_chain_gold" # Azure
location = "/mnt/demo/gold/"
table = "manufacturing_delay_alerts"

alerts.write.format("delta")\
    .option("path",f"{location}+{table}")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{database}.{table}")
