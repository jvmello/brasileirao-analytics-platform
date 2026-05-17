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


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/matches/"

    return spark.read.parquet(path)


def read_dim_team(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/dim_team/"

    return spark.read.parquet(path)


def read_dim_stadium(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/dim_stadium/"

    return spark.read.parquet(path)


def clean_text(column_name: str) -> F.Column:
    cleaned = F.regexp_replace(F.col(column_name).cast("string"), "\u00A0", " ")
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)

    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def clean_stadium_raw(column_name: str) -> F.Column:
    cleaned = F.regexp_replace(F.col(column_name).cast("string"), "\u00A0", " ")
    cleaned = F.trim(cleaned)

    # Remove markers like:
    # *(PF)
    # (*PF)
    # (PF)
    #  (*PF)
    cleaned = F.regexp_replace(
        cleaned,
        r"(?i)\s*\*?\s*\(\s*\*?\s*pf\s*\)\s*",
        "",
    )

    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.trim(cleaned)

    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


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

    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned)


def prepare_matches(silver_matches_df: DataFrame) -> DataFrame:
    return (
        silver_matches_df
        .withColumn("home_team_raw", clean_text("home_team"))
        .withColumn("away_team_raw", clean_text("away_team"))
        .withColumn("stadium_raw", clean_text("stadium"))
        .withColumn("stadium_clean", clean_stadium_raw("stadium"))
        .withColumn(
            "stadium_parts",
            F.split(F.col("stadium_clean"), r"\s*,\s*"),
        )
        .withColumn(
            "stadium_name_extracted",
            F.trim(F.element_at(F.col("stadium_parts"), 1)),
        )
        .withColumn(
            "stadium_name_key",
            build_text_key("stadium_name_extracted"),
        )
    )


def prepare_dim_team(dim_team_df: DataFrame) -> DataFrame:
    return (
        dim_team_df
        .select(
            F.col("id").alias("team_id"),
            clean_text("team_name_raw").alias("team_name_raw"),
        )
        .filter(F.col("team_name_raw").isNotNull())
        .dropDuplicates(["team_name_raw"])
    )


def prepare_dim_stadium(dim_stadium_df: DataFrame) -> DataFrame:
    return (
        dim_stadium_df
        .select(
            F.col("id").alias("stadium_id"),
            F.col("stadium_name_key"),
        )
        .filter(F.col("stadium_name_key").isNotNull())
        .groupBy("stadium_name_key")
        .agg(F.min("stadium_id").alias("stadium_id"))
    )


def transform_fact_matches(
    silver_matches_df: DataFrame,
    dim_team_df: DataFrame,
    dim_stadium_df: DataFrame,
) -> DataFrame:
    matches = prepare_matches(silver_matches_df).alias("m")

    teams = prepare_dim_team(dim_team_df)

    home_teams = (
        teams
        .select(
            F.col("team_id").alias("home_team_id"),
            F.col("team_name_raw").alias("home_team_raw_dim"),
        )
        .alias("ht")
    )

    away_teams = (
        teams
        .select(
            F.col("team_id").alias("away_team_id"),
            F.col("team_name_raw").alias("away_team_raw_dim"),
        )
        .alias("at")
    )

    stadiums = prepare_dim_stadium(dim_stadium_df).alias("s")

    joined = (
        matches
        .join(
            home_teams,
            F.col("m.home_team_raw") == F.col("ht.home_team_raw_dim"),
            "left",
        )
        .join(
            away_teams,
            F.col("m.away_team_raw") == F.col("at.away_team_raw_dim"),
            "left",
        )
        .join(
            stadiums,
            F.col("m.stadium_name_key") == F.col("s.stadium_name_key"),
            "left",
        )
    )

    return (
        joined
        .select(
            F.col("m.match_id"),
            F.col("m.round"),
            F.col("m.match_date"),
            F.col("m.match_time"),
            F.col("m.match_datetime"),
            F.col("m.season"),

            F.col("ht.home_team_id"),
            F.col("at.away_team_id"),
            F.col("s.stadium_id"),

            # Temporary debug columns. Remove later after all joins are stable.
            F.col("m.home_team_raw").alias("home_team"),
            F.col("m.away_team_raw").alias("away_team"),
            F.col("m.stadium_raw").alias("stadium"),
            F.col("m.stadium_clean"),
            F.col("m.stadium_name_key"),

            F.col("m.home_formation"),
            F.col("m.away_formation"),
            F.col("m.home_coach"),
            F.col("m.away_coach"),
            F.col("m.winner"),
            F.col("m.winner_normalized"),
            F.col("m.home_score"),
            F.col("m.away_score"),
            F.col("m.home_state"),
            F.col("m.away_state"),
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
            .otherwise(F.lit(None).cast(IntegerType())),
        )
        .withColumn(
            "match_points_away",
            F.when(F.col("away_result") == "win", F.lit(3))
            .when(F.col("away_result") == "draw", F.lit(1))
            .when(F.col("away_result") == "loss", F.lit(0))
            .otherwise(F.lit(None).cast(IntegerType())),
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


def show_invalid_foreign_keys(df: DataFrame) -> None:
    null_team_ids = df.filter(
        F.col("home_team_id").isNull()
        | F.col("away_team_id").isNull()
    )

    if null_team_ids.count() > 0:
        print("Rows with null team ids:")

        null_team_ids.select(
            "match_id",
            "season",
            "round",
            "match_date",
            "home_team",
            "away_team",
            "home_team_id",
            "away_team_id",
        ).show(200, truncate=False)

    null_stadium_ids = df.filter(F.col("stadium_id").isNull())

    if null_stadium_ids.count() > 0:
        print("Rows with null stadium_id:")

        null_stadium_ids.select(
            "match_id",
            "season",
            "round",
            "match_date",
            "home_team",
            "away_team",
            "stadium",
            "stadium_clean",
            "stadium_name_key",
        ).show(300, truncate=False)

        print("Distinct missing stadium keys:")

        null_stadium_ids.select(
            "stadium",
            "stadium_clean",
            "stadium_name_key",
        ).distinct().orderBy("stadium_name_key").show(300, truncate=False)


def validate_fact_matches(df: DataFrame) -> None:
    show_invalid_foreign_keys(df)

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

    checks.append(("null_home_team_id", df.filter(F.col("home_team_id").isNull()).count()))
    checks.append(("null_away_team_id", df.filter(F.col("away_team_id").isNull()).count()))
    checks.append(("null_stadium_id", df.filter(F.col("stadium_id").isNull()).count()))

    checks.append(
        (
            "same_home_away_team",
            df.filter(F.col("home_team_id") == F.col("away_team_id")).count(),
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

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["gold.fact_matches validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        raise ValueError("\n".join(lines))


def write_fact_matches(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_matches/"

    df.write.mode("overwrite").partitionBy("season").parquet(path)


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-fact-matches", config)

    try:
        silver_matches = read_silver_matches(spark, config)
        dim_team = read_dim_team(spark, config)
        dim_stadium = read_dim_stadium(spark, config)

        if silver_matches.limit(1).count() == 0:
            raise ValueError("No silver matches files found.")

        if dim_team.limit(1).count() == 0:
            raise ValueError("No gold dim_team files found.")

        if dim_stadium.limit(1).count() == 0:
            raise ValueError("No gold dim_stadium files found.")

        fact_matches = transform_fact_matches(
            silver_matches_df=silver_matches,
            dim_team_df=dim_team,
            dim_stadium_df=dim_stadium,
        )

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