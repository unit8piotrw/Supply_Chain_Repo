# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf, col, date_format, month, year, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

import string
import random
from faker import Faker
from datetime import datetime, timedelta
import configparser

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

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
    # fake customer data
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
    # Defining product categories and their likelihood of sale (1 being sold most frequently)
    demand_categories = {1: 'Very High', 2: 'High', 3: 'Medium', 4: 'Low', 5: 'Very Low'}
    
    # Defining base inventory levels based on the demand category (arbitrary scale)
    base_inventory_levels = {1: 100, 2: 80, 3: 60, 4: 40, 5: 20}

    # Random SKU function
    def generate_sku():
        letters = ''.join(random.choice(string.ascii_uppercase) for _ in range(2))
        numbers = ''.join(random.choice(string.digits) for _ in range(6))
        return letters + numbers

    products_data = []
    for product_id in range(1, num_products + 1):
        # choosing a demand category for the product (randomly for now?)
        category = random.choice(list(demand_categories.keys()))
        base_inventory = base_inventory_levels[category]
        # Randomizing inventory within a range, maintaining the trend
        inventory = random.randint(int(base_inventory * 0.7), int(base_inventory * 1.3))
        category_name = demand_categories[category]  # Get the category name as a string
        # making a random SKU for the product
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
    Creates a PySpark DataFrame with customer order data including a Date column with seasonal likelihoods.
    
    Args:
        num_orders (int): The number of orders to generate.
        customers_df (DataFrame): A DataFrame containing customer data.
        finished_goods_df (DataFrame): A DataFrame containing finished goods data

    Returns:
        DataFrame: A PySpark DataFrame with columns 'OrderID', 'CustomerID', 'productID', 'demandCategory', and 'Date'.
    """
    # Extracting customer IDs from customers_df
    customer_ids = [row['Customer ID'] for row in customers_df.collect()]

    # Calculating sale probabilities based on demandCategory
    demand_weights = {'Very High': 5, 'High': 4, 'Medium': 3, 'Low': 2, 'Very Low': 1}
    total_demand_weight = sum(demand_weights.values())

    def calculate_sale_probability(category):
        return demand_weights[category] / total_demand_weight
    # Creating a pyspark udf so that we can apply it to a whole column
    udf_calculate_sale_probability = udf(calculate_sale_probability, FloatType())

    # Adding saleProbability column to finished_goods_df
    finished_goods_df = finished_goods_df.withColumn(
        "saleProbability",
        udf_calculate_sale_probability(col("demandCategory"))
    )

    # Collecting product IDs and their sale probabilities
    product_data = finished_goods_df.select("productID", "saleProbability", "demandCategory").collect()
    product_ids = [row['productID'] for row in product_data]
    sale_probabilities = [row['saleProbability'] for row in product_data]

    def generate_seasonal_date():
        today = datetime.today()
        start_date = today.replace(year=today.year - 3)  # 3 years ago is the start point
        end_date = today
        date_generated = start_date + (end_date - start_date) * random.random()
        
        # seasons by month
        month = date_generated.month
        if month in [3, 4, 5, 9, 10, 11]:  # Spring and autumn
            return date_generated if random.random() < 0.70 else generate_seasonal_date()
        elif month in [6, 7, 8, 12, 1, 2]:  # Summer and winter
            return date_generated if random.random() < 0.30 else generate_seasonal_date()
        else:
            return date_generated

    def calculate_status(order_date):
        today = datetime.today()
        months_difference = (today.year - order_date.year) * 12 + today.month - order_date.month
        probability_closed = min(10 + 15 * months_difference, 100)  # Ensuring it does not exceed 100%
        return 'closed' if random.random() < (probability_closed / 100.0) else 'open'

    # Register the UDF
    udf_calculate_status = udf(calculate_status, StringType())

    # Generate order data with seasonal dates
    orders_data = []
    for order_id in range(1, num_orders + 1):
        customer_id = random.choice(customer_ids)
        product_id = random.choices(product_ids, weights=sale_probabilities, k=1)[0]
        # Finding the demandCategory for the selected product_id
        demand_category = next(row['demandCategory'] for row in product_data if row['productID'] == product_id)
        order_date = generate_seasonal_date()
        orders_data.append((order_id, customer_id, product_id, demand_category, order_date))

    orders_schema = ["OrderID", "CustomerID", "productID", "demandCategory", "Date"]
    customer_orders_df = spark.createDataFrame(orders_data, schema=orders_schema)
    customer_orders_df = customer_orders_df.withColumn('status', udf_calculate_status(col('Date')))
    customer_orders_df = customer_orders_df.withColumn('Date', date_format(col('Date'), 'yyyy-MM-dd'))

    return customer_orders_df

num_orders = 10000
customer_orders_df = create_customer_orders_df(num_orders, customers_df, finished_goods_df)
customer_orders_df.show()

# COMMAND ----------

# Checking if the orders were made correctly
demand_category_counts = customer_orders_df.groupBy('demandCategory').count().orderBy('count', ascending=False)
demand_category_counts_pd = demand_category_counts.toPandas()

# creating bar plt
plt.figure(figsize=(10, 6))
plt.bar(demand_category_counts_pd['demandCategory'], demand_category_counts_pd['count'], color='skyblue')
plt.xlabel('Demand Category')
plt.ylabel('Number of Orders')
plt.title('Number of Orders by Demand Category')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# Grouping by year and month and count the number of orders
monthly_order_counts = customer_orders_df.withColumn('Year', year('Date')) \
                                        . withColumn('Month', month('Date')) \
                                        . groupBy('Year', 'Month') \
                                        . agg(count('OrderID').alias('OrderCount')) \
                                        . orderBy('Year', 'Month')

monthly_order_counts_pd = monthly_order_counts.toPandas()
monthly_order_counts_pd['Date'] = pd.to_datetime(monthly_order_counts_pd[['Year', 'Month']].assign(DAY=1))

plt.figure(figsize=(12, 6))
plt.plot(monthly_order_counts_pd['Date'], monthly_order_counts_pd['OrderCount'], marker='o', linestyle='-')

# Formatting the x-axis to show dates in monthly increments
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
plt.gca().xaxis.set_major_locator(mdates.MonthLocator(interval=1))
plt.gcf().autofmt_xdate()  # Rotation

plt.xlabel('Date (Monthly Increments)')
plt.ylabel('Order Count')
plt.title('Order Count by Month')
plt.grid(True)

plt.show()

# COMMAND ----------


monthly_status_counts = customer_orders_df.groupBy(month('Date').alias('Month'), 'status').count()
open_orders_by_month = monthly_status_counts.filter(monthly_status_counts.status == 'open').orderBy('Month')
open_orders_by_month_pd = open_orders_by_month.toPandas()

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(open_orders_by_month_pd['Month'], open_orders_by_month_pd['count'], color='skyblue')
plt.xlabel('Month')
plt.ylabel('Count of Open Orders')
plt.title('Open Orders by Month')
plt.xticks(open_orders_by_month_pd['Month'])  # Ensure x-ticks are for each month
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

# COMMAND ----------

sorted_customer_orders_df = customer_orders_df.orderBy(col('Date').desc())

# Display the sorted DataFrame
sorted_customer_orders_df.show()

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
