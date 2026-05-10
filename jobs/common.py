from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def parse_match_minute(column_name: str) -> tuple[Column, Column, Column, Column]:
    minute_raw = F.trim(F.col(column_name).cast("string"))

    minute_base_str = F.regexp_extract(minute_raw, r"^(\d+)", 1)
    stoppage_str = F.regexp_extract(minute_raw, r"^\d+\+(\d+)$", 1)

    minute_base = F.when(minute_base_str == "", F.lit(None)).otherwise(
        minute_base_str.cast("int")
    )
    stoppage_minute = F.when(stoppage_str == "", F.lit(0)).otherwise(
        stoppage_str.cast("int")
    )

    minute_exact = F.when(minute_base.isNull(), F.lit(None)).otherwise(
        minute_base + stoppage_minute
    )

    return minute_raw, minute_base, stoppage_minute, minute_exact


from jobs.config import AppConfig


def filter_latest_load(df: DataFrame, explicit_load_date: str | None) -> DataFrame:
    if explicit_load_date:
        filtered = df.filter(F.col("_load_date") == explicit_load_date)
        if filtered.limit(1).count() == 0:
            raise ValueError(
                f"No bronze files found for load_date={explicit_load_date}"
            )
        return filtered

    latest_load_date = df.select(
        F.max("_load_date").alias("latest_load_date")
    ).collect()[0]["latest_load_date"]

    if not latest_load_date:
        raise ValueError(
            "Could not determine latest load_date from bronze matches files."
        )

    return df.filter(F.col("_load_date") == latest_load_date)


def build_spark_session(app_name: str, config: AppConfig) -> SparkSession:
    """
    Assumes your Spark environment already supports S3A/MinIO.
    If your image does not include hadoop-aws, keep using the same Spark image/setup
    you already used in the online-retail project.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # S3 config
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", config.s3_endpoint_url)
        .config("spark.hadoop.fs.s3a.endpoint.region", config.aws_region)
        .config("spark.hadoop.fs.s3a.access.key", config.aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", config.aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # credenciais
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )

        # 👉 JARS (ESSENCIAL)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,org.postgresql:postgresql:42.7.3"
        )

        # timeouts
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")

        .getOrCreate()
    )

    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf = spark._jsc.hadoopConfiguration()

    # timeouts (todos)
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.establish.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.request.timeout", "60000")
    hadoop_conf.set("fs.s3a.socket.timeout", "60000")

    # pooling / threads
    hadoop_conf.set("fs.s3a.connection.maximum", "100")
    hadoop_conf.set("fs.s3a.threads.max", "20")

    # TTL / idle (CRÍTICO)
    hadoop_conf.set("fs.s3a.connection.ttl", "300000")
    hadoop_conf.set("fs.s3a.connection.idle.time", "60000")

    # retries
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")
    hadoop_conf.set("fs.s3a.retry.limit", "3")

    hadoop_conf.set("fs.s3a.connection.maximum", "1")
    hadoop_conf.set("fs.s3a.threads.max", "1")

    conf = spark.sparkContext._jsc.hadoopConfiguration()

    conf.set("fs.s3a.threads.keepalivetime", "60000")
    conf.set("fs.s3a.multipart.purge.age", "86400000")

    conf = spark.sparkContext._jsc.hadoopConfiguration()
    spark.sparkContext.setLogLevel("WARN")

    return spark


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(
        config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")
    ).rstrip("/")


def normalize_string(column_name: str) -> F.Column:
    """
    Trims values, removes non-breaking spaces, and converts empty strings to null.
    """
    cleaned = F.trim(F.regexp_replace(F.col(column_name), "\u00a0", " "))
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def read_bronze_layer(spark: SparkSession, config: AppConfig, source: str) -> DataFrame:
    bronze_path = f"s3a://{config.bucket_name}/{config.bronze_raw_prefix.rstrip('/')}/source={source}/"

    df = (
        spark.read.option("header", "true")
        .option("multiLine", "false")
        .option("recursiveFileLookup", "true")
        .csv(bronze_path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn(
            "_load_date",
            F.regexp_extract(
                F.col("_source_file"), r"load_date=(\d{4}-\d{2}-\d{2})", 1
            ),
        )
    )

    return df
