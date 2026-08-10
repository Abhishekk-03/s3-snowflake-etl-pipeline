from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("S3_Write")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)