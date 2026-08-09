from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("S3_Write")
    .master("local[*]")
    .config(
        "spark.jars",
        "jars/hadoop-aws-3.3.4.jar,"
        "jars/aws-java-sdk-bundle-1.12.262.jar"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .getOrCreate()
)