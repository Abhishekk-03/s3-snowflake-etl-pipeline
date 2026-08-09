
# Impoerting Required Libraries
from utils.checkpoint import Updated_Customer, Updated_Category, Updated_Employee, Updated_product, valid_columns, expected_schema
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import col, trim, when, initcap, to_date, row_number, lit
from pyspark.sql.window import Window


def check_missing_col(df,valid_columns):
    #Check missing columns

    actual_columns = set(df.columns)

    #Cheking for missing columns
    missing_columns = []
    for colo in valid_columns:
        if colo not in actual_columns:
            missing_columns.append(colo)

    return missing_columns

def clean_date(df):
    df = df.withColumn("Order_Date", to_date(col("Order_Date"), "M/d/yyyy"))
    df = df.withColumn("Delivery_Date", to_date(col("Delivery_Date"), "M/d/yyyy"))

    return df   

def clean_customers(df):
    df = df.withColumn("Customer_ID", col("Customer_ID").cast(StringType()))
    df = df.withColumn("Customer_ID",trim(col("Customer_ID")))
    df = df.withColumn("Customer_ID", when(col("Customer_ID").isin(Updated_Customer),col("Customer_ID")).otherwise(None))
    
    df = df.withColumn("Customer_Name", col("Customer_Name").cast(StringType()))             
    df = df.withColumn("Customer_Name", trim(col("Customer_Name")))
    df = df.withColumn("Customer_Name", initcap(col("Customer_Name")))
    df = df.withColumn("Customer_Name", when(col("Customer_Name").isNull(), None).otherwise(col("Customer_Name")))
    df = df.withColumn("Customer_Name", when(col("Customer_Name") == "Nan", None).otherwise(col("Customer_Name")))

    return df

def clean_employee(df):
    df = df.withColumn("Employee_ID", col("Employee_ID").cast(StringType()))
    df = df.withColumn("Employee_ID",trim(col("Employee_ID")))
    df = df.withColumn("Employee_ID", when(col("Employee_ID").isin(Updated_Employee),col("Employee_ID")).otherwise(None))
    
    df = df.withColumn("Employee_Name", col("Employee_Name").cast(StringType()))             
    df = df.withColumn("Employee_Name", trim(col("Employee_Name")))
    df = df.withColumn("Employee_Name",when(col("Employee_Name") == "", None).otherwise(col("Employee_Name")))
    df = df.withColumn("Employee_Name", initcap(col("Employee_Name")))

    return df


def clean_product(df):
    df = df.withColumn("Product_ID", col("Product_ID").cast(StringType()))
    df = df.withColumn("Product_ID",trim(col("Product_ID")))
    df = df.withColumn("Product_ID", when(col("Product_ID").isin(Updated_product),col("Product_ID")).otherwise(None))

    df = df.withColumn("Product_Name", col("Product_Name").cast(StringType()))             
    df = df.withColumn("Product_Name", trim(col("Product_Name")))
    df = df.withColumn("Product_Name",when(col("Product_Name") == "", None).otherwise(col("Product_Name")))
    df = df.withColumn("Product_Name", initcap(col("Product_Name")))
    df = df.withColumn("Product_Name", when(col("Product_Name") == "Nan", None).otherwise(col("Product_Name")))

    return df


def clean_category(df):
    df = df.withColumn("Category_ID", col("Category_ID").cast(StringType()))
    df = df.withColumn("Category_ID",trim(col("Category_ID")))
    df = df.withColumn("Category_ID",when(col("Category_ID").isin(Updated_Category),col("Category_ID")).otherwise(None))
    
    df = df.withColumn("Category_Name", col("Category_Name").cast(StringType()))             
    df = df.withColumn("Category_Name", trim(col("Category_Name")))
    df = df.withColumn("Category_Name",when(col("Category_Name") == "", None).otherwise(col("Category_Name")))
    df = df.withColumn("Category_Name", initcap(col("Category_Name")))

    return df


def clean_quantity(df):
    df = df.withColumn("Quantity", trim(col("Quantity")))
    df = df.withColumn("Quantity", col("Quantity").cast(IntegerType()))
    df = df.withColumn("Quantity",when((col("Quantity") < 0) | (col("Quantity").isNull()), None ).otherwise(col("Quantity")))

    return df

def clean_price(df):
    df = df.withColumn("Price", trim(col("Price")))
    df = df.withColumn("Price", col("Price").cast(IntegerType()))
    df = df.withColumn("Price",when((col("Price") < 0) | (col("Price").isNull()),None).otherwise(col("Price")))

    return df


def clean_amount(df):
    df = df.withColumn("Sale", trim(col("Sale")))
    df = df.withColumn("Sale", col("Sale").cast(IntegerType()))
    df = df.withColumn("Sale",when((col("Sale") < 0) | (col("Sale").isNull()), None).otherwise(col("Sale")))

    return df


def split_valid_invalid_rows(df):

    # Adding row_number column to identify duplicate rows
    df = df.withColumn("row_number", row_number().over(Window.partitionBy(*df.columns).orderBy(lit(1))))

    #filtering the rows with null values in any of the columns
    rejected_rows = df.filter(
    col("Order_Date").isNull() |
    col("Delivery_Date").isNull() |
    col("Customer_ID").isNull() |
    col("Customer_Name").isNull() |
    col("Employee_ID").isNull() |
    col("Employee_Name").isNull() |
    col("Product_ID").isNull() |
    col("Product_Name").isNull() |
    col("Category_ID").isNull() |
    col("Category_Name").isNull() |
    col("Quantity").isNull() |
    col("Price").isNull() |
    col("Sale").isNull() |
    (col("row_number") > 1))

    df = df.filter(
    col("Order_Date").isNotNull() &
    col("Delivery_Date").isNotNull() &
    col("Customer_ID").isNotNull() &
    col("Customer_Name").isNotNull() &
    col("Employee_ID").isNotNull() &
    col("Employee_Name").isNotNull() &
    col("Product_ID").isNotNull() &
    col("Product_Name").isNotNull() &
    col("Category_ID").isNotNull() &
    col("Category_Name").isNotNull() &
    col("Quantity").isNotNull() &
    col("Price").isNotNull() &
    col("Sale").isNotNull() &
    (col("row_number") == 1))

    # Dropping the row_number column from both DataFrames
    rejected_rows = rejected_rows.drop("row_number")
    df = df.drop("row_number")

    return df, rejected_rows

def validate_schema(df, expected_schema):

    actual_schema = {i.name: i.dataType for i in df.schema.fields}

    mismatched_data_type = []
    for col_name, D_type in expected_schema.items():
        if actual_schema[col_name] != D_type:
            mismatched_data_type.append(col_name)

    return mismatched_data_type


def transform(df):

    # Stop Execution if there are missing columns
    missing_columns = check_missing_col(df,valid_columns)
    if missing_columns:
        raise Exception(f"Stop Execution due to missing columns: {missing_columns}")

    df = clean_date(df)
    df = clean_customers(df)
    df = clean_employee(df)
    df = clean_product(df)
    df = clean_category(df)
    df = clean_quantity(df)
    df = clean_price(df)
    df = clean_amount(df)

    clean_df, rejected_df = split_valid_invalid_rows(df)

    # Stop Execution if there are missing data_type
    mismatched_data_type = validate_schema(clean_df,expected_schema)
    if mismatched_data_type:
        raise Exception(f"Stop Execution due to mismatched data types: {mismatched_data_type}")
    
    return clean_df, rejected_df