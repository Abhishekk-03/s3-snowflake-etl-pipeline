import boto3
import json
from botocore.exceptions import ClientError
from utils.spark_session import spark

DATA_BUCKET = "my-csv-data-obj-2026"
DATA_PREFIX = "bronze-layer-csv/sales-2026/"
CHECKPOINT_BUCKET = "checkpoint-obj"
CHECKPOINT_KEY = "csv-checkpoint/csv-checkpoint.json"


def read_data():
    s3_client = boto3.client("s3")

    # 1. List all CSV files
    paginator = s3_client.get_paginator("list_objects_v2")
    all_files = []
    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix=DATA_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                all_files.append(obj["Key"])

    # 2. Load checkpoint (first run par file nahi hogi)
    try:
        response = s3_client.get_object(Bucket=CHECKPOINT_BUCKET, Key=CHECKPOINT_KEY)
        body = response["Body"].read().decode("utf-8")
        store_key = set(json.loads(body)) if body.strip() else set()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            store_key = set()
        else:
            raise

    # 3. Find new files
    unprocessed_files = [f for f in all_files if f not in store_key]

    if not unprocessed_files:
        print("There is no file to process")
        return None, [], store_key

    paths = [f"s3://{DATA_BUCKET}/{f}" for f in unprocessed_files]
    df = spark.read.option("header", True).csv(paths)

    return df, unprocessed_files, store_key