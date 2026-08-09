from pyspark.sql.types import IntegerType, StringType, DateType

Updated_Customer = [
    "Cust_02", "Cust_05", "Cust_06", "Cust_08", "Cust_03",
    "Cust_01", "Cust_10", "Cust_09", "Cust_07"
]

Updated_Employee = [
    "VT_8", "VT_1", "VT_3", "VT_6",
    "VT_7", "VT_4", "VT_2", "VT_5"
]

Updated_product = [
    "P_10", "P_9", "P_7", "P_3", "P_13", "P_14", "P_12",
    "P_6", "P_4", "P_5", "P_1", "P_11", "P_8", "P_2"
]

Updated_Category = [
    "C_1", "C_3", "C_2"
]

valid_columns = ["Order_Date", "Delivery_Date", "Customer_ID", 
                 "Customer_Name", "Employee_ID", "Employee_Name", "Product_ID", 
                 "Product_Name", "Category_ID", "Category_Name",
                   "Quantity", "Price", "Sale"]

expected_schema = { 
    "Order_Date": DateType(),
    "Delivery_Date": DateType(), 
    "Customer_ID": StringType(),
    "Customer_Name": StringType(),
    "Employee_ID": StringType(),
    "Employee_Name": StringType(), 
    "Product_ID": StringType(),
    "Product_Name": StringType(),
    "Category_ID": StringType(),
    "Category_Name": StringType(),
    "Quantity": IntegerType(),
    "Price": IntegerType(),
    "Sale": IntegerType()}