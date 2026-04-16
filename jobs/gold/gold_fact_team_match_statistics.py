from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from jobs.config import AppConfig
from jobs.common import build_spark_session


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_match_statistics(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/match_statistics/"
    return spark.read.parquet(path)


def transform_fact_team_match_statistics(df: DataFrame) -> DataFrame:
    return (
        df.select(
            "match_id",
            "round",
            "season",
            "match_date",
            "team",
            "opponent_team",
            "is_home_team",
            "match_result",
            "goals_scored",
            "goals_conceded",
            "shots",
            "shots_on_target",
            "shot_accuracy",
            "scoring_efficiency",
            "possession",
            "passes",
            "pass_accuracy",
            "fouls",
            "yellow_cards",
            "red_cards",
            "offsides",
            "corners",
        )
        .withColumn(
            "match_side",
            F.when(F.col("is_home_team") == True, F.lit("home"))
             .when(F.col("is_home_team") == False, F.lit("away"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "match_points",
            F.when(F.col("match_result") == "win", F.lit(3))
             .when(F.col("match_result") == "draw", F.lit(1))
             .when(F.col("match_result") == "loss", F.lit(0))
             .otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn("win_flag", F.when(F.col("match_result") == "win", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("draw_flag", F.when(F.col("match_result") == "draw", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("loss_flag", F.when(F.col("match_result") == "loss", F.lit(1)).otherwise(F.lit(0)))
        .withColumn("clean_sheet_flag", F.when(F.col("goals_conceded") == 0, F.lit(1)).otherwise(F.lit(0)))
    )


def validate_fact_team_match_statistics(df: DataFrame) -> None:
    checks = []

    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team", df.filter(F.col("team").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(("null_match_result", df.filter(F.col("match_result").isNull()).count()))
    checks.append(("same_team_opponent", df.filter(F.col("team") == F.col("opponent_team")).count()))
    checks.append((
        "duplicate_match_team",
        df.groupBy("match_id", "team").count().filter(F.col("count") > 1).count(),
    ))
    checks.append((
        "rows_per_match_not_equal_2",
        df.groupBy("match_id").count().filter(F.col("count") != 2).count(),
    ))
    checks.append((
        "invalid_match_side",
        df.filter(~F.col("match_side").isin("home", "away")).count(),
    ))
    checks.append((
        "negative_goals",
        df.filter((F.col("goals_scored") < 0) | (F.col("goals_conceded") < 0)).count(),
    ))

    failing = [(name, count) for name, count in checks if count > 0]
    if failing:
        lines = ["gold.fact_team_match_statistics validation failed:"]
        lines.extend([f"- {name}: {count} invalid rows/groups" for name, count in failing])
        raise ValueError("\n".join(lines))


def write_fact_team_match_statistics(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_team_match_statistics/"
    (
        df.write
        .mode("overwrite")
        .partitionBy("season")
        .parquet(path)
    )


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-fact-team-match-statistics", config)

    try:
        silver_stats = read_silver_match_statistics(spark, config)

        if silver_stats.limit(1).count() == 0:
            raise ValueError("No silver match_statistics files found.")

        fact_stats = transform_fact_team_match_statistics(silver_stats)
        validate_fact_team_match_statistics(fact_stats)
        write_fact_team_match_statistics(fact_stats, config)

        print("gold.fact_team_match_statistics successfully written.")
        print(f"rows={fact_stats.count()}")
        print(f"distinct_matches={fact_stats.select('match_id').distinct().count()}")
        print(f"distinct_teams={fact_stats.select('team').distinct().count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()