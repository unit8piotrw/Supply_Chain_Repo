# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

import string
import random
from faker import Faker
from datetime import datetime, timedelta
import configparser

fake = Faker('de_CH')
Faker.seed(42)
random.seed(42)

spark = SparkSession.builder.getOrCreate()


config = configparser.ConfigParser()
config.read('config.ini')
password = config.get("credentials", "pwd")

# Define the database connection properties

database = 'supply_chain_demo_DB'
server = 'supply-chain-demo-db'

username = "CloudSAfc49655c"
jdbcUrl = f"jdbc:sqlserver://supply-chain-demo-db.database.windows.net:1433;database={database};user={username}@supply-chain-demo-db;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# Simulating customers_df

def create_customers_df(num_customers):
    """
    Creates a PySpark DataFrame with fake customer data.
    Assumes PySpark and Faker are already initialized
    Args:
        num_customers (int): The number of customers to generate.
    Returns:
        DataFrame: A PySpark DataFrame with columns 'Customer ID', 'Address', and 'Company Name'.
    """
    # Generate fake customer data
    customer_data = []
    for x in range(num_customers):
        tup = (str(x), fake.address(), fake.company())
        customer_data.append(tup)

    customers_df = spark.createDataFrame(customer_data, ['Customer ID', 'Address', 'Company Name'])

    return customers_df

customers_df = create_customers_df(20)
customers_df.show(20)

# COMMAND ----------

def create_final_goods_df(num_products):
    """
    Creates a PySpark DataFrame with fake product data.
    
    Args:
        num_products (int): The number of products to generate.

    Returns:
        DataFrame: A PySpark DataFrame with columns 'productID', 'productSKU', 'inventory', 'demandCategory'.
    """
    # Define product categories and their likelihood of sale (1 being sold most frequently)
    demand_categories = {1: 'Very High', 2: 'High', 3: 'Medium', 4: 'Low', 5: 'Very Low'}
    
    # Define base inventory levels based on the demand category (arbitrary scale)
    base_inventory_levels = {1: 100, 2: 80, 3: 60, 4: 40, 5: 20}

    # Function to generate a random SKU
    def generate_sku():
        letters = ''.join(random.choice(string.ascii_uppercase) for _ in range(2))
        numbers = ''.join(random.choice(string.digits) for _ in range(6))
        return letters + numbers

    # Generate product data with random inventory within the trend
    products_data = []
    for product_id in range(1, num_products + 1):
        # Randomly choose a demand category for the product
        category = random.choice(list(demand_categories.keys()))
        base_inventory = base_inventory_levels[category]
        # Randomize inventory within a range, maintaining the trend
        inventory = random.randint(int(base_inventory * 0.7), int(base_inventory * 1.3))
        category_name = demand_categories[category]  # Get the category name as a string
        # Generate random SKU for the product
        product_sku = generate_sku()
        products_data.append(Row(productID=product_id, productSKU=product_sku, inventory=inventory, demandCategory=category_name))

    # Create DataFrame
    finished_goods_df = spark.createDataFrame(products_data)

    return finished_goods_df

num_products = 30
finished_goods_df = create_final_goods_df(num_products)

finished_goods_df.show()

# COMMAND ----------

def create_customer_orders_df(num_orders, customers_df, finished_goods_df):
    """
    Creates a PySpark DataFrame with customer order data.
    
    Args:
        num_orders (int): The number of orders to generate.
        customers_df (DataFrame): A DataFrame containing customer data.
        finished_goods_df (DataFrame): A DataFrame containing finished goods data

    Returns:
        DataFrame: A PySpark DataFrame with columns 'OrderID', 'CustomerID', 'productID', and 'demandCategory'.
    """
    # Extracting customer IDs from customers_df
    customer_ids = [row['Customer ID'] for row in customers_df.collect()]

    # Calculate sale probabilities based on demandCategory
    demand_weights = {'Very High': 5, 'High': 4, 'Medium': 3, 'Low': 2, 'Very Low': 1}
    total_demand_weight = sum(demand_weights.values())

    def calculate_sale_probability(category):
        return demand_weights[category] / total_demand_weight
    udf_calculate_sale_probability = udf(calculate_sale_probability, FloatType())

    # Add saleProbability column to finished_goods_df
    finished_goods_df = finished_goods_df.withColumn(
        "saleProbability",
        udf_calculate_sale_probability(col("demandCategory"))
    )

    # Collect product IDs and their sale probabilities
    product_data = finished_goods_df.select("productID", "saleProbability", "demandCategory").collect()
    product_ids = [row['productID'] for row in product_data]
    sale_probabilities = [row['saleProbability'] for row in product_data]

    # Generate order data
    orders_data = []
    for order_id in range(1, num_orders + 1):
        customer_id = random.choice(customer_ids)
        product_id = random.choices(product_ids, weights=sale_probabilities, k=1)[0]
        # Finding the demandCategory for the selected product_id
        demand_category = next(row['demandCategory'] for row in product_data if row['productID'] == product_id)
        orders_data.append((order_id, customer_id, product_id, demand_category))

    # Create DataFrame
    orders_schema = ["OrderID", "CustomerID", "productID", "demandCategory"]
    customer_orders_df = spark.createDataFrame(orders_data, schema=orders_schema)

    return customer_orders_df


num_orders = 1000
customer_orders_df = create_customer_orders_df(num_orders, customers_df, finished_goods_df)
customer_orders_df.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Checking if the orders were made correctly
demand_category_counts = customer_orders_df.groupBy('demandCategory').count().orderBy('count', ascending=False)
demand_category_counts_pd = demand_category_counts.toPandas()

# Create the bar plot
plt.figure(figsize=(10, 6))
plt.bar(demand_category_counts_pd['demandCategory'], demand_category_counts_pd['count'], color='skyblue')
plt.xlabel('Demand Category')
plt.ylabel('Number of Orders')
plt.title('Number of Orders by Demand Category')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Writing data in the database (at end)

"""
table = "table1"
customers_df.write \
   . format("jdbc") \
   . option("url", jdbcUrl) \
   . option("dbtable", table) \
   . option("user", username) \
   . option("password", password) \
   . mode("overwrite") \
   . save()

"""
