# Databricks notebook source
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Define the database connection properties
password = config.get("credentials", "pwd")
username = config.get("credentials", "dbuser")

database = 'supply_chain_demo_DB'
server = 'supply-chain-demo-db'

jdbcUrl = f"jdbc:sqlserver://supply-chain-demo-db.database.windows.net:1433;database={database};user={username}@supply-chain-demo-db;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"



# COMMAND ----------

# This part is only here to create the connection between our blob storage and databricks (mount the storage)
"""
container_name = "supplychain"
storage_account = "supplychaindemo"
sas_token = config.get("credentials", "sas", raw=True)
blob_url = "wasbs://" + container_name + "@" + storage_account + ".blob.core.windows.net/"

dbutils.fs.mount(
    source=blob_url,
    mount_point="/mnt/demo",
    extra_configs={"fs.azure.sas." + container_name + "." + storage_account + ".blob.core.windows.net": sas_token}
)
"""

# COMMAND ----------

# Reading raw data from database, saving them as bronze layer in blob storage, registering in databricks data catalog

database = "supply_chain_bronze"
location = "/mnt/demo/bronze/"
tables = ["customers", "customer_orders", "finished_goods", "manufacturing_processes", "manufacturing_process_parts", "material_master"]

count = 0
for table in tables:
    # Read data from database table
    df = spark.read.format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", tables[count]) \
        .option("user", username) \
        .option("password", password) \
        .load()
    
    # Overwrite existing Delta table schema and save as Delta table
    df.write.format("delta")\
        .option("path",f"{location}+{tables[count]}")\
        .mode("overwrite")\
        .option("overwriteSchema", "true")\
        .saveAsTable(f"{database}.{tables[count]}")

    count += 1
