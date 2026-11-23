from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("S3_Write")
    .master('local[*]')
    .getOrCreate()
)