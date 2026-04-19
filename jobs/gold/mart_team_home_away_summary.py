from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_fact_team_match_statistics(
    spark: SparkSession, config: AppConfig
) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_team_match_statistics/"
    return spark.read.parquet(path)


def transform_team_home_away_summary(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("season", "team", "match_side")
        .agg(
            F.count("*").alias("matches"),
            F.sum("win_flag").alias("wins"),
            F.sum("draw_flag").alias("draws"),
            F.sum("loss_flag").alias("losses"),
            F.sum("goals_scored").alias("goals_for"),
            F.sum("goals_conceded").alias("goals_against"),
            F.sum("match_points").alias("points"),
            F.avg("goals_scored").alias("avg_goals_scored"),
            F.avg("goals_conceded").alias("avg_goals_conceded"),
            F.avg("shots").alias("avg_shots"),
            F.avg("possession").alias("avg_possession"),
            F.avg("pass_accuracy").alias("avg_pass_accuracy"),
        )
        .withColumn("goal_difference", F.col("goals_for") - F.col("goals_against"))
        .withColumn(
            "points_pct",
            F.when(F.col("matches") == 0, F.lit(None)).otherwise(
                F.col("points") / (F.col("matches") * F.lit(3))
            ),
        )
    )


def write_team_home_away_summary(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/marts/team_home_away_summary/"
    (df.write.mode("overwrite").partitionBy("season").parquet(path))


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("mart-team-home-away-summary", config)

    try:
        fact_stats = read_fact_team_match_statistics(spark, config)
        mart = transform_team_home_away_summary(fact_stats)
        write_team_home_away_summary(mart, config)

        print("gold.marts.team_home_away_summary successfully written.")
        print(f"rows={mart.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
