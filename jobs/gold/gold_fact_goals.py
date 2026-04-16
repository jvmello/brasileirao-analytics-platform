from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.config import AppConfig
from jobs.common import build_spark_session


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_goals(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/goals/"
    return spark.read.parquet(path)


def transform_fact_goals(df: DataFrame) -> DataFrame:
    return df.select(
        "goal_id",
        "match_id",
        "round",
        "season",
        "match_date",
        "team",
        "player",
        "minute_raw",
        "minute_base",
        "stoppage_minute",
        "minute",
        "minute_bucket",
        "goal_type",
        "is_home_team_goal",
    )


def validate_fact_goals(df: DataFrame) -> None:
    checks = []

    checks.append(("null_goal_id", df.filter(F.col("goal_id").isNull()).count()))
    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team", df.filter(F.col("team").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append((
        "duplicate_goal_id",
        df.groupBy("goal_id").count().filter(F.col("count") > 1).count(),
    ))
    checks.append(("negative_minute", df.filter(F.col("minute") < 0).count()))

    failing = [(name, count) for name, count in checks if count > 0]
    if failing:
        lines = ["gold.fact_goals validation failed:"]
        lines.extend([f"- {name}: {count} invalid rows/groups" for name, count in failing])
        raise ValueError("\n".join(lines))


def write_fact_goals(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_goals/"
    (
        df.write
        .mode("overwrite")
        .partitionBy("season")
        .parquet(path)
    )


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-fact-goals", config)

    try:
        silver_goals = read_silver_goals(spark, config)

        if silver_goals.limit(1).count() == 0:
            raise ValueError("No silver goals files found.")

        fact_goals = transform_fact_goals(silver_goals)
        validate_fact_goals(fact_goals)
        write_fact_goals(fact_goals, config)

        print("gold.fact_goals successfully written.")
        print(f"rows={fact_goals.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()