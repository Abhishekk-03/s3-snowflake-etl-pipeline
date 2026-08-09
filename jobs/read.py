import boto3
import json
from utils.spark_session import spark

def get_s3_objects():
#     session = boto3.Session(
#         aws_access_key_id = "",
#         aws_secret_access_key = "",
#         region_name=""
# )

    s3_client = boto3.client("s3")

    response = s3_client.list_objects_v2(
        Bucket="practice-bucket-etl",
        Prefix = "Sale-2026/")

    return response

def get_processed_files():

    try:
        with open("checkpoint.json", "r") as file:
            existing_files = set(json.load(file))

    except FileNotFoundError:
        existing_files =  set()

    return existing_files
    
def find_new_files(response, existing_files):

    unprocesed_files = []
    for obj in response["Contents"]:
        value = obj["Key"]

        if value.endswith(".csv") and value not in existing_files:
            unprocesed_files.append(value)
        
    return unprocesed_files

def read_spark_data(unprocesed_files):

    #stop reading if no new file
    if not unprocesed_files:
        raise Exception("There is no file to process")

    path = [f"s3a://practice-bucket-etl/{file}" for file in unprocesed_files]
    
    df = spark.read.format("CSV")\
            .option("header",True)\
            .load(path)

    return df

def read_data():

    response = get_s3_objects()
    existing_files = get_processed_files()
    unprocessed_files = find_new_files(response,existing_files)
    df = read_spark_data(unprocessed_files)

    return df, unprocessed_files, existing_files