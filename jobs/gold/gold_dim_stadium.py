from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"s3a://{config.bucket_name}/{get_silver_prefix(config)}/matches/"
    return spark.read.parquet(path)


def read_stadium_mapping(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"{config.seeds_path.rstrip('/')}/stadium_mapping.csv"

    return (
        spark.read
        .option("header", "true")
        .csv(path)
        .select(
            F.trim(F.col("stadium_raw")).alias("stadium_raw"),
            F.trim(F.col("stadium_name")).alias("stadium_name"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("state")).alias("state"),
        )
    )


def transform_dim_stadium(matches_df: DataFrame, stadium_mapping_df: DataFrame) -> DataFrame:
    stadiums = (
        matches_df
        .select(F.trim(F.col("stadium")).alias("stadium_raw"))
        .filter(F.col("stadium_raw").isNotNull())
        .dropDuplicates(["stadium_raw"])
    )

    mapped = stadiums.join(stadium_mapping_df, on="stadium_raw", how="left")

    missing = mapped.filter(
        F.col("stadium_name").isNull()
        | F.col("city").isNull()
        | F.col("state").isNull()
    )

    if missing.count() > 0:
        missing.show(truncate=False)
        raise ValueError("There are stadiums without mapping.")

    window = Window.orderBy("stadium_raw")

    return (
        mapped
        .withColumn("id", F.dense_rank().over(window).cast("long"))
        .select(
            "id",
            "stadium_raw",
            "stadium_name",
            "city",
            "state",
        )
    )


def write_dim_stadium(df: DataFrame, config: AppConfig) -> None:
    path = f"s3a://{config.bucket_name}/{get_gold_prefix(config)}/dim_stadium/"
    df.write.mode("overwrite").parquet(path)


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-dim-stadium", config)

    try:
        silver_matches = read_silver_matches(spark, config)
        stadium_mapping = read_stadium_mapping(spark, config)

        dim_stadium = transform_dim_stadium(silver_matches, stadium_mapping)

        write_dim_stadium(dim_stadium, config)

        print("gold.dim_stadium successfully written.")
        print(f"rows={dim_stadium.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()