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

# Defining manufacturing smulation function
# We assume that the order of df_jobs is the order in which to execute the jobs in

"""
df_jobs = df_jobs.filter(col("FacilityID") == "Facility 2")
df_jobs = df_jobs.sort(col("FacilityID"), col("OrderYearMonth"), col("ProcessPartID"))  


def manufacture(df_jobs):
    
    current_time = {"Facility 1": 0, "Facility 2": 0, "Facility 3": 0, "Facility 4": 0}
    output_data = []
    # Plan: Split it by facility, start time at 0 and simply convert later
    for row in df_jobs.collect():
        # Calculate start time and end time
        start_time = current_time[row["FacilityID"]]
        end_time = start_time + (row["ProcessingTime"] * row["Delta_produced"])

        # Updating current time for each facility
        current_time[row["FacilityID"]] = end_time

        # Create output row
        output_row = (row["ProductID"], row["OrderYearMonth"], row["Delta_produced"], row["MaterialID"],
                      row["ProcessPartID"], row["Req_Prod_Quant"], row["FacilityID"], start_time, end_time)
        
        output_data.append(output_row)

    output_df = spark.createDataFrame(output_data, ["ProductID", "OrderYearMonth", "Delta_produced", "MaterialID",
                                                    "ProcessPartID", "Req_Prod_Quant", "FacilityID", "StartTime", "EndTime"])
    
    return output_df

output = manufacture(df_jobs)
"""

# COMMAND ----------

from pyspark.sql.functions import lag, greatest, sum, to_date, add_months, date_diff, current_date, expr
from pyspark.sql import Window

df_jobs = spark.sql("SELECT * FROM supply_chain_gold.manufacturing_job_list")


def schedule_jobs(df_jobs, max_hours_day):
    # Define window

    # Changed 2 lines
    # df_jobs = df_jobs.withColumn("OrderDateT", to_date(df_jobs["OrderYearMonth"], "yyyy-MM"))
    df_jobs = df_jobs.withColumn("OrderDeadline", add_months(df_jobs["OrderYearMonth"], 3))


    df_jobs = df_jobs.withColumn("CurrentDate", current_date())

    df_jobs = df_jobs.withColumn("DiffHours", date_diff(df_jobs["OrderDeadline"], df_jobs["CurrentDate"]) * max_hours_day)
    
    window = Window.partitionBy("FacilityID").orderBy("OrderYearMonth", "ProductID")
    df_jobs = df_jobs.withColumn("TotalProcessingTime", col("ProcessingTime") * col("Delta_produced"))

    df_jobs = df_jobs.withColumn('Running_Total', sum('TotalProcessingTime').over(window))

    alerts = df_jobs.filter(col("Running_Total") > col("DiffHours"))
    alerts = alerts.withColumn("ExpectedDelay",col("Running_Total") - col("DiffHours"))

    delay_category_expr = "CASE "
    for i in range(20, 220, 20):
        delay_category_expr += f"WHEN ExpectedDelay <= {i} THEN '<{i}' "
    delay_category_expr += "ELSE '200+' END"
    alerts = alerts.withColumn("DelayCategory", expr(delay_category_expr))

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
