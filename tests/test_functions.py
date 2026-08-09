from jobs.read import find_new_files, read_spark_data
from jobs.transformation import check_missing_col
import pytest
import pandas as pd



# Test_1 - When we have 2 unprocessed files 
def test_find_new_files():

    response = {
        "Contents": [
            {"Key": "Sale-2026/jan.csv"},
            {"Key": "Sale-2026/feb.csv"},
            {"Key": "Sale-2026/march.csv"},
            {"Key": "Sale-2026/april.csv"}
        ]
    }
    existing_files = {
        "Sale-2026/jan.csv",
        "Sale-2026/feb.csv"
    }

    result =  find_new_files(response, existing_files)
    assert result == ["Sale-2026/march.csv","Sale-2026/april.csv"]

# Test_2 - When we have no new files
def test_find_new_files_2():

    response = {
        "Contents": [
            {"Key": "Sale-2026/jan.csv"},
            {"Key": "Sale-2026/feb.csv"}
        ]
    }
    existing_files = {
        "Sale-2026/jan.csv",
        "Sale-2026/feb.csv"
    }

    result2 =  find_new_files(response, existing_files)
    assert result2 == []

# Test_3 - When we have different file extention
def test_find_new_files_3():

    response = {
        "Contents": [
            {"Key": "Sale-2026/jan.csv"},
            {"Key": "Sale-2026/feb.csv"},
            {"Key": "Sale-2026/april.txt"}
        ]
    }
    existing_files = {
        "Sale-2026/jan.csv",
        "Sale-2026/feb.csv"
    }

    result3 =  find_new_files(response, existing_files)
    assert result3 == []

#test_4 when we dont have file
def test_read_spark_data_no_files():

    unprocessed_files = []
    with pytest.raises(Exception, match="There is no file to process"):
        read_spark_data(unprocessed_files)


#test_5 When we have all valid columns
def test_check_missing_col_1():

    valid_columns = ["Order_Date", "Delivery_Date", "Customer_ID", 
                 "Customer_Name", "Employee_ID", "Employee_Name", "Product_ID", 
                 "Product_Name", "Category_ID", "Category_Name",
                   "Quantity", "Price", "Sale"]

    data = {
    "Order_Date": ["2026-01-01", "2026-01-02", "2026-01-03"],
    "Delivery_Date": ["2026-01-05", "2026-01-06", "2026-01-07"],
    "Customer_ID": [1001, 1002, 1003],
    "Customer_Name": ["Rahul", "Amit", "Priya"],
    "Employee_ID": [101, 102, 103],
    "Employee_Name": ["Abhi", "Karan", "Rohan"],
    "Product_ID": ["P_001", "P_002", "P_003"],
    "Product_Name": ["Laptop", "Mouse", "Keyboard"],
    "Category_ID": [1, 2, 2],
    "Category_Name": ["Electronics", "Accessories", "Accessories"],
    "Quantity": [2, 5, 3],
    "Price": [50000.0, 800.0, 1500.0],
    "Sale": [100000.0, 4000.0, 4500.0]
}

    df = pd.DataFrame(data)

    result = check_missing_col(df, valid_columns)

    assert result == []


#test_5 When we have some invalid columns
def test_check_missing_col_2():

    valid_columns = ["Order_Date", "Delivery_Date", "Customer_ID", 
                 "Customer_Name", "Employee_ID", "Employee_Name", "Product_ID", 
                 "Product_Name", "Category_ID", "Category_Name",
                   "Quantity", "Price", "Sale"]

    data = {
    "Order_Date": ["2026-01-01", "2026-01-02", "2026-01-03"],
    "Delivery_Date": ["2026-01-05", "2026-01-06", "2026-01-07"],
    "Customer_ID": ['1001', '1002', '1003'],
    "Customer_Name": ["Rahul", "Amit", "Priya"],
    "Employee_ID": ['101', '102', '103'],
    "Employee_Name": ["Abhi", "Karan", "Rohan"],
    "Product": ["P_001", "P_002", "P_003"],
    "Product_Name": ["Laptop", "Mouse", "Keyboard"],
    "Category_ID": ['1', '2', '2'],
    "Category": ["Electronics", "Accessories", "Accessories"],
    "Quantity": [2, 5, 3],
    "Price": [50000.0, 800.0, 1500.0],
    "Sale": [100000.0, 4000.0, 4500.0]
}

    df = pd.DataFrame(data)
    result = check_missing_col(df, valid_columns)
    assert result == ['Product_ID','Category_Name']