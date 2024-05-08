# Databricks notebook source
def replace_newlines(df: DataFrame, *columns: str) -> DataFrame:
    # Replacing newline characters with a space in the specified columns of a DataFrame.
    # For loop here is unavoidable
    for column_name in columns:
        df = df.withColumn(column_name, regexp_replace(col(column_name), "\n", " "))
    return df

def remove_duplicate_rows(df: DataFrame) -> DataFrame:
    # Removing duplicate rows from a DataFrame.
    return df.dropDuplicates()

def convert_column_types(df: DataFrame) -> DataFrame:
    # Converting column data types in a PySpark DataFrame:
    for column_name in df.columns:
        # Getting the data type of the current column
        dtype = df.schema[column_name].dataType
        
        # Checking and casting the column to the new data type if necessary
        if isinstance(dtype, (LongType, ByteType, ShortType)):
            df = df.withColumn(column_name, col(column_name).cast(IntegerType()))
        elif isinstance(dtype, (DecimalType, DoubleType)):
            df = df.withColumn(column_name, col(column_name).cast(FloatType()))
        elif isinstance(dtype, (CharType, VarcharType)):
            df = df.withColumn(column_name, col(column_name).cast(StringType()))
    return df

def convert_date_format(df: DataFrame) -> DataFrame:
    for column in df.columns:
        dtype = df.schema[column].dataType
        # Check if the data type is a date or timestamp
        if isinstance(dtype, (DateType, TimestampType)):
            # Convert the column to date format 'yyyy-MM-dd'
            df = df.withColumn(column, col(column).cast('date'))
    return df

def fill_empty_fields(df: DataFrame) -> DataFrame:
    # Define default fill values for the specified data types
    fill_values = {
        'string': '',
        'int': 0,
        'float': 0.0
    }

    # Iterate over the DataFrame's columns and fill them with the appropriate value
    for column_name, data_type in df.dtypes:
        if data_type in fill_values:
            df = df.fillna({column_name: fill_values[data_type]}, subset=[column_name])

    return df

