# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# create a pyspark dataframe called customers with the columns: Customer ID, Address, Name
from pyspark.sql import SparkSession
from faker import Faker

fake = Faker()
spark = SparkSession.builder.getOrCreate()


# Define the database connection properties
database = 'supply_chain_demo_DB'
server = 'supply-chain-demo-db'
jdbcUrl = f"jdbc:sqlserver://{server};database={database};"

password = "Databrick678"
username = "CloudSAfc49655c"
jdbcUrl = f"jdbc:sqlserver://supply-chain-demo-db.database.windows.net:1433;database={database};user={username}@supply-chain-demo-db;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# Simulating customer data
customer_data = []
for x in range(30):
    tup = (str(x), fake.address(), fake.name())
    customer_data.append(tup)

customers = spark.createDataFrame(customer_data, ['Customer ID', 'Address', 'Name'])

# COMMAND ----------

# Writing data in the database

customers.write \
   . format("jdbc") \
   . option("url", jdbcUrl) \
   . option("dbtable", table) \
   . option("user", username) \
   . option("password", password) \
   . mode("overwrite") \
   . save()
