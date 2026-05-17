from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from jobs.common import build_spark_session
from jobs.config import AppConfig


def clean_stadium_raw(column_name: str) -> F.Column:
    cleaned = F.col(column_name)

    # Replace non-breaking spaces with normal spaces
    cleaned = F.regexp_replace(cleaned, "\u00A0", " ")

    # Trim normal spaces
    cleaned = F.trim(cleaned)

    # Remove markers like *(PF), (*PF), (PF)
    cleaned = F.regexp_replace(
        cleaned,
        r"(?i)\s*\*?\s*\(\s*\*?\s*pf\s*\)\s*",
        "",
    )

    # Collapse duplicated spaces
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")

    return F.trim(cleaned)

def remove_accents(column: F.Column) -> F.Column:
    accented = "áàãâäéèêëíìîïóòõôöúùûüçñÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇÑ"
    unaccented = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"

    return F.translate(column, accented, unaccented)


def build_text_key(column_name: str) -> F.Column:
    cleaned = F.lower(F.trim(F.col(column_name)))
    cleaned = remove_accents(cleaned)
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.regexp_replace(cleaned, r"[^a-z0-9]+", "_")
    cleaned = F.regexp_replace(cleaned, r"(^_+|_+$)", "")

    return cleaned

def get_silver_prefix(config: AppConfig) -> str:
    return getattr(
        config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")
    ).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"s3a://{config.bucket_name}/{get_silver_prefix(config)}/matches/"
    return spark.read.parquet(path)


def read_stadium_mapping(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"{config.seeds_path.rstrip('/')}/stadium_mapping.csv"

    return (
        spark.read.option("header", "true")
        .csv(path)
        .select(
            F.trim(F.col("stadium_raw")).alias("stadium_raw"),
            F.trim(F.col("stadium_name")).alias("stadium_name"),
            F.trim(F.col("city")).alias("city"),
            F.trim(F.col("state")).alias("state"),
        )
    )

def read_stadium_mapping(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"{config.seeds_path.rstrip('/')}/stadium_mapping.csv"

    return (
        spark.read
        .option("header", "true")
        .csv(path)
        .select(
            F.trim(F.col("stadium_name_key")).alias("stadium_name_key"),
            F.trim(F.col("stadium_name")).alias("mapped_stadium_name"),
            F.trim(F.col("city")).alias("mapped_city"),
            F.trim(F.col("state")).alias("mapped_state"),
        )
    )

def validate_dim_stadium(df: DataFrame) -> None:
    missing_location = df.filter(
        F.col("city").isNull()
        | F.col("state").isNull()
    )

    if missing_location.count() > 0:
        missing_location.select(
            "stadium_raw",
            "stadium_name_key",
            "stadium_name",
            "city",
            "state",
        ).show(200, truncate=False)

        missing_location.select(
            "stadium_raw",
        ).show(200, truncate=False)

        raise ValueError("There are stadiums without city or state.")

def transform_dim_stadium(
    matches_df: DataFrame,
    stadium_mapping_df: DataFrame,
) -> DataFrame:
    stadiums = (
        matches_df
        .select(F.trim(F.col("stadium")).alias("stadium_raw"))
        .filter(F.col("stadium_raw").isNotNull())
        .filter(F.col("stadium_raw") != "")
        .withColumn("stadium_clean", clean_stadium_raw("stadium_raw"))
        .dropDuplicates(["stadium_clean"])
    )

    parsed = (
        stadiums
        .withColumn(
            "stadium_parts",
            F.split(F.col("stadium_clean"), r"\s*,\s*")
        )
        .withColumn(
            "stadium_name_extracted",
            F.trim(F.element_at(F.col("stadium_parts"), 1))
        )
        .withColumn(
            "city_extracted",
            F.when(
                F.size(F.col("stadium_parts")) >= 2,
                F.trim(F.element_at(F.col("stadium_parts"), 2))
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "state_extracted",
            F.when(
                F.size(F.col("stadium_parts")) >= 3,
                F.trim(F.element_at(F.col("stadium_parts"), 3))
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "stadium_name_key",
            build_text_key("stadium_name_extracted")
        )
    )

    mapped = (
        parsed
        .join(stadium_mapping_df, on="stadium_name_key", how="left")
        .withColumn(
            "stadium_name",
            F.coalesce(
                F.col("mapped_stadium_name"),
                F.col("stadium_name_extracted")
            )
        )
        .withColumn(
            "city",
            F.coalesce(
                F.col("city_extracted"),
                F.col("mapped_city")
            )
        )
        .withColumn(
            "state",
            F.coalesce(
                F.col("state_extracted"),
                F.col("mapped_state")
            )
        )
    )

    deduplicated = (
        mapped
        .groupBy("stadium_name_key")
        .agg(
            F.first("stadium_raw", ignorenulls=True).alias("stadium_raw"),
            F.first("stadium_clean", ignorenulls=True).alias("stadium_clean"),
            F.first("stadium_name", ignorenulls=True).alias("stadium_name"),
            F.first("city", ignorenulls=True).alias("city"),
            F.first("state", ignorenulls=True).alias("state"),
        )
    )

    window = Window.orderBy("stadium_name_key")

    return (
        deduplicated
        .withColumn("id", F.row_number().over(window).cast("long"))
        .select(
            "id",
            "stadium_raw",
            "stadium_clean",
            "stadium_name_key",
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
