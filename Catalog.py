# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS supply_chain_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS supply_chain CASCADE
# MAGIC

# COMMAND ----------

dbutils.fs.unmount("/mnt/deltalake")

# COMMAND ----------

dbutils.fs.unmount("/mnt/demo")

# COMMAND ----------

from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# Set the Azure Blob Storage account details
container_name = "supplychain"
storage_account = "scdeltalake"
sas_token = config.get("credentials", "sas2", raw=True)
blob_url = "wasbs://" + container_name + "@" + storage_account + ".blob.core.windows.net/"

# Mount the Azure Delta Lake directory
dbutils.fs.mount(
    source=blob_url,
    mount_point="/mnt/demo",
    extra_configs={"fs.azure.sas." + container_name + "." + storage_account + ".blob.core.windows.net": sas_token}
)
