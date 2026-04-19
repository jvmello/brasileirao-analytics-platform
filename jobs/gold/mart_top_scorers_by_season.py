from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_fact_goals(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_goals/"
    return spark.read.parquet(path)


def transform_top_scorers(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("player").isNotNull())
        .groupBy("season", "team", "player")
        .agg(
            F.count("*").alias("goals"),
            F.avg("minute").alias("avg_goal_minute"),
            F.sum(F.when(F.col("goal_type") == "penalti", 1).otherwise(0)).alias(
                "penalty_goals"
            ),
            F.sum(F.when(F.col("goal_type") == "gol_contra", 1).otherwise(0)).alias(
                "own_goals_labeled"
            ),
        )
    )


def write_top_scorers(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/marts/top_scorers_by_season/"
    (df.write.mode("overwrite").partitionBy("season").parquet(path))


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("mart-top-scorers-by-season", config)

    try:
        fact_goals = read_fact_goals(spark, config)
        mart = transform_top_scorers(fact_goals)
        write_top_scorers(mart, config)

        print("gold.marts.top_scorers_by_season successfully written.")
        print(f"rows={mart.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
