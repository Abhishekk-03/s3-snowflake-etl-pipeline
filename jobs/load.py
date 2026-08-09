import json

def create_load_tables(clean_df, rejected_df):

    # Customer Dimension
    dim_customer = (
        clean_df.select(
            "Customer_ID",
            "Customer_Name"
        ).dropDuplicates()
    )

    # Product Dimension
    dim_product = (
        clean_df.select(
            "Product_ID",
            "Product_Name",
            "Category_ID"
        ).dropDuplicates()
    )

    dim_employee = (clean_df.select(
        "Employee_ID",
        "Employee_Name").dropDuplicates()
    )

    dim_category = (clean_df.select(
        "Category_ID",
        "Category_Name"
        ).dropDuplicates()
    )

    sales_fact = (clean_df.select(
        "Order_Date",
        "Delivery_Date",
        "Customer_ID",
        "Employee_ID",
        "Product_ID",
        "Category_ID",
        "Quantity",
        "Price",
        "Sale").dropDuplicates()
    )


    dim_customer.write \
    .mode("overwrite") \
    .parquet("s3a://cleaned-parquet-data/customer-parquet/")

    dim_employee.write \
    .mode("overwrite") \
    .parquet("s3a://cleaned-parquet-data/employee-parquet/")

    dim_category.write \
    .mode("overwrite") \
    .parquet("s3a://cleaned-parquet-data/category-parquet/")

    dim_product.write \
    .mode("overwrite") \
    .parquet("s3a://cleaned-parquet-data/product-parquet/")

    sales_fact.write \
    .mode("overwrite") \
    .parquet("s3a://cleaned-parquet-data/fact-parquet/")

    rejected_df.write \
    .mode("overwrite") \
    .parquet("s3a://cleaned-parquet-data/rejected-parquet/")


def update_checkpoint(store_key, unprocessed_files):

    # Update checkpoint
    store_key.update(unprocessed_files)
    with open("checkpoint.json", "w") as file:
        json.dump(list(store_key), file, indent=4)