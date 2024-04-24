# Databricks notebook source
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
password = config.get("credentials", "pwd")

# Define the database connection properties
database = 'supply_chain_demo_DB'
server = 'supply-chain-demo-db'
jdbcUrl = f"jdbc:sqlserver://{server};database={database};"

username = "CloudSAfc49655c"
jdbcUrl = f"jdbc:sqlserver://supply-chain-demo-db.database.windows.net:1433;database={database};user={username}@supply-chain-demo-db;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"



# COMMAND ----------

df = spark.read.format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", "table1") \
    .option("user", username) \
    .option("password", password) \
    .load()

df.show(5)
