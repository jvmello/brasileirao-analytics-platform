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


def read_dim_team(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"s3a://{config.bucket_name}/{get_gold_prefix(config)}/dim_team/"
    return spark.read.parquet(path)


def read_dim_stadium(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"s3a://{config.bucket_name}/{get_gold_prefix(config)}/dim_stadium/"
    return spark.read.parquet(path)


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    silver_matches_path = f"s3a://{config.bucket_name}/{silver_prefix}/matches/"

    return spark.read.parquet(silver_matches_path)


def transform_fact_matches(
    silver_matches_df: DataFrame,
    dim_team_df: DataFrame,
    dim_stadium_df: DataFrame,
) -> DataFrame:
    home_team = dim_team_df.select(
        F.col("id").alias("home_team_id"),
        F.col("team_name_raw").alias("home_team_raw"),
    )

    away_team = dim_team_df.select(
        F.col("id").alias("away_team_id"),
        F.col("team_name_raw").alias("away_team_raw"),
    )

    stadium = dim_stadium_df.select(
        F.col("id").alias("stadium_id"),
        F.col("stadium_raw"),
    )

    return (
        silver_matches_df.alias("m")
        .join(
            home_team.alias("ht"),
            F.col("m.home_team") == F.col("ht.home_team_raw"),
            "left",
        )
        .join(
            away_team.alias("at"),
            F.col("m.away_team") == F.col("at.away_team_raw"),
            "left",
        )
        .join(stadium.alias("s"), F.col("m.stadium") == F.col("s.stadium_raw"), "left")
        .select(
            F.col("m.match_id"),
            F.col("m.round"),
            F.col("m.match_date"),
            F.col("m.match_time"),
            F.col("m.match_datetime"),
            F.col("m.season"),
            F.col("s.stadium_id"),
            F.col("ht.home_team_id"),
            F.col("at.away_team_id"),
            F.col("m.home_formation"),
            F.col("m.away_formation"),
            F.col("m.home_coach"),
            F.col("m.away_coach"),
            F.col("m.winner"),
            F.col("m.winner_normalized"),
            F.col("m.home_score"),
            F.col("m.away_score"),
            F.col("m.gross_revenue"),
            F.col("m.is_draw"),
            F.col("m.home_result"),
            F.col("m.away_result"),
            F.col("m.total_goals"),
        )
        .withColumn(
            "match_points_home",
            F.when(F.col("home_result") == "win", F.lit(3))
            .when(F.col("home_result") == "draw", F.lit(1))
            .when(F.col("home_result") == "loss", F.lit(0))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "match_points_away",
            F.when(F.col("away_result") == "win", F.lit(3))
            .when(F.col("away_result") == "draw", F.lit(1))
            .when(F.col("away_result") == "loss", F.lit(0))
            .otherwise(F.lit(None)),
        )
        .withColumn(
            "home_win_flag",
            F.when(F.col("home_result") == "win", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "away_win_flag",
            F.when(F.col("away_result") == "win", F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "draw_flag",
            F.when(F.col("is_draw") == True, F.lit(1)).otherwise(F.lit(0)),
        )
    )


def validate_fact_matches(df: DataFrame) -> None:
    checks = []

    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(
        (
            "duplicate_match_id",
            df.groupBy("match_id").count().filter(F.col("count") > 1).count(),
        )
    )
    checks.append(("null_match_date", df.filter(F.col("match_date").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(("null_home_team", df.filter(F.col("home_team").isNull()).count()))
    checks.append(("null_away_team", df.filter(F.col("away_team").isNull()).count()))
    checks.append(
        (
            "same_home_away_team",
            df.filter(F.col("home_team") == F.col("away_team")).count(),
        )
    )
    checks.append(
        (
            "null_scores",
            df.filter(
                F.col("home_score").isNull() | F.col("away_score").isNull()
            ).count(),
        )
    )
    checks.append(
        (
            "negative_scores",
            df.filter((F.col("home_score") < 0) | (F.col("away_score") < 0)).count(),
        )
    )
    checks.append(
        (
            "total_goals_mismatch",
            df.filter(
                F.col("total_goals") != (F.col("home_score") + F.col("away_score"))
            ).count(),
        )
    )
    checks.append(
        (
            "is_draw_mismatch",
            df.filter(
                F.col("is_draw") != (F.col("home_score") == F.col("away_score"))
            ).count(),
        )
    )
    checks.append(
        (
            "home_points_mismatch",
            df.filter(
                F.col("match_points_home")
                != F.when(F.col("home_result") == "win", F.lit(3))
                .when(F.col("home_result") == "draw", F.lit(1))
                .when(F.col("home_result") == "loss", F.lit(0))
            ).count(),
        )
    )
    checks.append(
        (
            "away_points_mismatch",
            df.filter(
                F.col("match_points_away")
                != F.when(F.col("away_result") == "win", F.lit(3))
                .when(F.col("away_result") == "draw", F.lit(1))
                .when(F.col("away_result") == "loss", F.lit(0))
            ).count(),
        )
    )
    checks.append(
        (
            "draw_flag_mismatch",
            df.filter(
                F.col("draw_flag")
                != F.when(F.col("is_draw") == True, F.lit(1)).otherwise(F.lit(0))
            ).count(),
        )
    )

    checks.append(
        ("null_home_team_id", df.filter(F.col("home_team_id").isNull()).count())
    )
    checks.append(
        ("null_away_team_id", df.filter(F.col("away_team_id").isNull()).count())
    )
    checks.append(("null_stadium_id", df.filter(F.col("stadium_id").isNull()).count()))

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["gold.fact_matches validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        raise ValueError("\n".join(lines))


def write_fact_matches(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    gold_path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_matches/"

    (df.write.mode("overwrite").partitionBy("season").parquet(gold_path))


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-fact-matches", config)

    try:
        silver_matches = read_silver_matches(spark, config)
        dim_team = read_dim_team(spark, config)
        dim_stadium = read_dim_stadium(spark, config)

        fact_matches = transform_fact_matches(
            silver_matches,
            dim_team,
            dim_stadium,
        )

        if silver_matches.limit(1).count() == 0:
            raise ValueError("No silver matches files found.")

        fact_matches = transform_fact_matches(silver_matches)
        validate_fact_matches(fact_matches)
        write_fact_matches(fact_matches, config)

        print("gold.fact_matches successfully written.")
        print(f"rows={fact_matches.count()}")
        print(f"distinct_matches={fact_matches.select('match_id').distinct().count()}")
        print(f"distinct_seasons={fact_matches.select('season').distinct().count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
