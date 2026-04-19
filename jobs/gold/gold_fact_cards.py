from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(
        config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")
    ).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_cards(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/cards/"
    return spark.read.parquet(path)


def transform_fact_cards(df: DataFrame) -> DataFrame:
    return df.select(
        "card_id",
        "match_id",
        "round",
        "season",
        "match_date",
        "team",
        "player",
        "shirt_number",
        "position",
        "minute_raw",
        "minute_base",
        "stoppage_minute",
        "minute",
        "minute_bucket",
        "card_type",
        "card_weight",
        "is_home_team_card",
    )


def validate_fact_cards(df: DataFrame) -> None:
    checks = []

    checks.append(("null_card_id", df.filter(F.col("card_id").isNull()).count()))
    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team", df.filter(F.col("team").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(
        (
            "duplicate_card_id",
            df.groupBy("card_id").count().filter(F.col("count") > 1).count(),
        )
    )
    checks.append(
        (
            "invalid_card_type",
            df.filter(~F.col("card_type").isin("yellow", "red")).count(),
        )
    )
    checks.append(("negative_minute", df.filter(F.col("minute") < 0).count()))

    failing = [(name, count) for name, count in checks if count > 0]
    if failing:
        lines = ["gold.fact_cards validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        raise ValueError("\n".join(lines))


def write_fact_cards(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_cards/"
    (df.write.mode("overwrite").partitionBy("season").parquet(path))


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-fact-cards", config)

    try:
        silver_cards = read_silver_cards(spark, config)

        if silver_cards.limit(1).count() == 0:
            raise ValueError("No silver cards files found.")

        fact_cards = transform_fact_cards(silver_cards)
        validate_fact_cards(fact_cards)
        write_fact_cards(fact_cards, config)

        print("gold.fact_cards successfully written.")
        print(f"rows={fact_cards.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
