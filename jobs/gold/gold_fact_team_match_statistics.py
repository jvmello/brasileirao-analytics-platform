from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(
        config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")
    ).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_match_statistics(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/match_statistics/"
    return spark.read.parquet(path)


def transform_fact_team_match_statistics(
    df: DataFrame,
    dim_team_df: DataFrame,
    fact_matches_df: DataFrame,
) -> DataFrame:
    teams = dim_team_df.select(
        F.col("id").alias("team_id"),
        F.col("team_name_raw"),
    )

    matches = fact_matches_df.select(
        "match_id",
        "home_team_id",
        "away_team_id",
    )

    return (
        df.alias("s")
        .join(teams.alias("t"), F.col("s.team") == F.col("t.team_name_raw"), "left")
        .join(matches.alias("m"), F.col("s.match_id") == F.col("m.match_id"), "left")
        .withColumn(
            "opponent_team_id",
            F.when(
                F.col("t.team_id") == F.col("m.home_team_id"), F.col("m.away_team_id")
            )
            .when(
                F.col("t.team_id") == F.col("m.away_team_id"), F.col("m.home_team_id")
            )
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "match_side",
            F.when(F.col("is_home_team") == True, F.lit("home"))
            .when(F.col("is_home_team") == False, F.lit("away"))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "match_points",
            F.when(F.col("match_result") == "win", F.lit(3))
            .when(F.col("match_result") == "draw", F.lit(1))
            .when(F.col("match_result") == "loss", F.lit(0))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "win_flag",
            F.when(F.col("match_result") == "win", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "draw_flag",
            F.when(F.col("match_result") == "draw", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "loss_flag",
            F.when(F.col("match_result") == "loss", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "clean_sheet_flag",
            F.when(F.col("goals_conceded") == 0, F.lit(1)).otherwise(F.lit(0)),
        )
        .select(
            F.col("s.match_id"),
            F.col("s.round"),
            F.col("s.season"),
            F.col("s.match_date"),
            F.col("t.team_id"),
            F.col("opponent_team_id"),
            F.col("s.team"),
            F.col("s.opponent_team"),
            F.col("s.is_home_team"),
            F.col("s.match_result"),
            F.col("s.goals_scored"),
            F.col("s.goals_conceded"),
            F.col("s.shots"),
            F.col("s.shots_on_target"),
            F.col("s.shot_accuracy"),
            F.col("s.scoring_efficiency"),
            F.col("s.possession"),
            F.col("s.passes"),
            F.col("s.pass_accuracy"),
            F.col("s.fouls"),
            F.col("s.yellow_cards"),
            F.col("s.red_cards"),
            F.col("s.offsides"),
            F.col("s.corners"),
            F.col("match_side"),
            F.col("match_points"),
            F.col("win_flag"),
            F.col("draw_flag"),
            F.col("loss_flag"),
            F.col("clean_sheet_flag"),
        )
    )


def validate_fact_team_match_statistics(df: DataFrame) -> None:
    checks = []

    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team", df.filter(F.col("team").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(
        ("null_match_result", df.filter(F.col("match_result").isNull()).count())
    )
    checks.append(
        (
            "same_team_opponent",
            df.filter(F.col("team") == F.col("opponent_team")).count(),
        )
    )
    checks.append(
        (
            "duplicate_match_team",
            df.groupBy("match_id", "team").count().filter(F.col("count") > 1).count(),
        )
    )
    checks.append(
        (
            "rows_per_match_not_equal_2",
            df.groupBy("match_id").count().filter(F.col("count") != 2).count(),
        )
    )
    checks.append(
        (
            "invalid_match_side",
            df.filter(~F.col("match_side").isin("home", "away")).count(),
        )
    )
    checks.append(
        (
            "negative_goals",
            df.filter(
                (F.col("goals_scored") < 0) | (F.col("goals_conceded") < 0)
            ).count(),
        )
    )

    failing = [(name, count) for name, count in checks if count > 0]
    if failing:
        lines = ["gold.fact_team_match_statistics validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        raise ValueError("\n".join(lines))


def write_fact_team_match_statistics(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_team_match_statistics/"
    (df.write.mode("overwrite").partitionBy("season").parquet(path))


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
