from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from jobs.config import AppConfig

def build_spark_session(app_name: str, config: AppConfig) -> SparkSession:
    """
    Assumes your Spark environment already supports S3A/MinIO.
    If your image does not include hadoop-aws, keep using the same Spark image/setup
    you already used in the online-retail project.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.endpoint.region", config.aws_region)
        .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")).rstrip("/")


def normalize_string(column_name: str) -> F.Column:
    """
    Trims values, removes non-breaking spaces, and converts empty strings to null.
    """
    cleaned = F.trim(F.regexp_replace(F.col(column_name), "\u00A0", " "))
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)

def read_bronze_layer(spark: SparkSession, config: AppConfig, source: str) -> DataFrame:
    bronze_path = f"s3a://{config.bucket_name}/{config.bronze_raw_prefix.rstrip('/')}/source={source}/"

    df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "false")
        .option("recursiveFileLookup", "true")
        .csv(bronze_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_load_date",
            F.regexp_extract(F.col("_source_file"), r"load_date=(\d{4}-\d{2}-\d{2})", 1)
        )
    )

    return df