from __future__ import annotations

import argparse
import os
import unicodedata

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType
from pyspark.sql.window import Window

from jobs.common import (
    build_spark_session,
    filter_latest_load,
    get_silver_prefix,
    normalize_string,
    parse_match_minute,
    read_bronze_layer,
)
from jobs.config import AppConfig


def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return value.strip().lower().replace(" ", "_").replace("-", "_")


def resolve_column(df: DataFrame, *candidates: str) -> str:
    normalized_map = {normalize_name(col): col for col in df.columns}

    for candidate in candidates:
        candidate_key = normalize_name(candidate)
        if candidate_key in normalized_map:
            return normalized_map[candidate_key]

    raise KeyError(
        f"Could not resolve any of these columns: {candidates}. "
        f"Available columns: {df.columns}"
    )


def parse_numeric(column_name: str) -> F.Column:
    cleaned = F.regexp_replace(
        F.trim(F.col(column_name).cast("string")), r"[^0-9\-]", ""
    )
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned.cast(IntegerType()))


def minute_bucket_expr(column_name: str) -> F.Column:
    col = F.col(column_name)
    return (
        F.when(col.isNull(), F.lit(None))
        .when(col <= 15, F.lit("00_15"))
        .when(col <= 30, F.lit("16_30"))
        .when(col <= 45, F.lit("31_45"))
        .when(col <= 60, F.lit("46_60"))
        .when(col <= 75, F.lit("61_75"))
        .when(col <= 90, F.lit("76_90"))
        .otherwise(F.lit("90_plus"))
    )


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    silver_matches_path = f"s3a://{config.bucket_name}/{silver_prefix}/matches/"

    return spark.read.parquet(silver_matches_path).select(
        "match_id",
        "season",
        "match_date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "total_goals",
    )


def transform_goals(raw_df: DataFrame, silver_matches_df: DataFrame) -> DataFrame:
    match_id_col = resolve_column(raw_df, "partida_id", "partida_ID", "match_id")
    round_col = resolve_column(raw_df, "rodada", "round")
    team_col = resolve_column(raw_df, "clube", "team")
    player_col = resolve_column(raw_df, "atleta", "player")
    minute_col = resolve_column(raw_df, "minuto", "minute")
    minute_raw, minute_base, stoppage_minute, minute_exact = parse_match_minute(
        minute_col
    )
    goal_type_col = resolve_column(raw_df, "tipo_de_gol", "tipo gol", "goal_type")

    canonical = raw_df.select(
        F.col(match_id_col).cast(LongType()).alias("match_id"),
        F.col(round_col).cast(IntegerType()).alias("round"),
        normalize_string(team_col).alias("team"),
        normalize_string(player_col).alias("player"),
        minute_raw.alias("minute_raw"),
        minute_base.alias("minute_base"),
        stoppage_minute.alias("stoppage_minute"),
        minute_exact.alias("minute_exact"),
        normalize_string(goal_type_col).alias("goal_type"),
        F.to_date(F.col("_load_date"), "yyyy-MM-dd").alias("ingestion_date"),
        F.col("_source_file").alias("source_file"),
    )

    window_spec = Window.partitionBy(
        "match_id", "team", "player", "minute_exact", "goal_type"
    ).orderBy("source_file")

    canonical = canonical.withColumn("goal_sequence", F.row_number().over(window_spec))

    enriched = (
        canonical.alias("goals")
        .join(
            silver_matches_df.alias("matches"),
            on="match_id",
            how="left",
        )
        .withColumn(
            "is_home_team_goal",
            F.when(F.col("team") == F.col("home_team"), F.lit(True))
            .when(F.col("team") == F.col("away_team"), F.lit(False))
            .otherwise(F.lit(None).cast(BooleanType())),
        )
        .withColumn("minute_bucket", minute_bucket_expr("minute_exact"))
        .withColumn(
            "goal_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("match_id").cast("string"),
                    F.coalesce(F.col("team"), F.lit("")),
                    F.coalesce(F.col("player"), F.lit("")),
                    F.coalesce(F.col("minute_exact").cast("string"), F.lit("")),
                    F.coalesce(F.col("goal_type"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn(
            "goal_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("match_id").cast("string"),
                    F.coalesce(F.col("team"), F.lit("")),
                    F.coalesce(F.col("player"), F.lit("")),
                    F.coalesce(F.col("minute_exact").cast("string"), F.lit("")),
                    F.coalesce(F.col("goal_type"), F.lit("")),
                    F.col("goal_sequence").cast("string"),
                ),
                256,
            ),
        )
        .select(
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
            F.col("minute_exact").alias("minute"),
            "minute_bucket",
            "goal_type",
            "is_home_team_goal",
            "ingestion_date",
            "source_file",
        )
    )

    return enriched


def validate_silver_goals(df: DataFrame, silver_matches_df: DataFrame) -> None:
    checks = []

    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team", df.filter(F.col("team").isNull()).count()))
    checks.append(("null_player", df.filter(F.col("player").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(
        (
            "invalid_team_for_match",
            df.filter(F.col("is_home_team_goal").isNull()).count(),
        )
    )
    checks.append(("null_minute", df.filter(F.col("minute_exact").isNull()).count()))
    checks.append(("negative_minute", df.filter(F.col("minute_exact") < 0).count()))
    checks.append(("minute_too_high", df.filter(F.col("minute_exact") > 130).count()))
    checks.append(
        (
            "duplicate_goal_id",
            df.groupBy("goal_id").count().filter(F.col("count") > 1).count(),
        )
    )

    goal_counts = (
        df.groupBy("match_id").count().withColumnRenamed("count", "goal_events")
    )

    match_goal_counts = (
        silver_matches_df.select("match_id", "total_goals")
        .join(goal_counts, on="match_id", how="left")
        .fillna({"goal_events": 0})
        .filter(F.col("goal_events") != F.col("total_goals"))
        .count()
    )

    checks.append(("goal_count_mismatch_vs_matches", match_goal_counts))

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["silver.goals validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        # raise ValueError("\n".join(lines))


def write_silver_goals(df: DataFrame, config: AppConfig) -> None:
    silver_prefix = get_silver_prefix(config)
    silver_path = f"s3a://{config.bucket_name}/{silver_prefix}/goals/"

    (df.write.mode("overwrite").partitionBy("season").parquet(silver_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver.goals from bronze raw goals."
    )
    parser.add_argument(
        "--load-date",
        required=False,
        help="Optional load_date to process in YYYY-MM-DD format. Defaults to latest bronze load.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig()
    spark = build_spark_session("silver-goals", config)

    try:
        bronze_goals = read_bronze_layer(spark, config, "goals")

        if bronze_goals.limit(1).count() == 0:
            raise ValueError("No bronze goals files found.")

        latest_or_selected = filter_latest_load(bronze_goals, args.load_date)
        silver_matches = read_silver_matches(spark, config)

        silver_goals = transform_goals(
            raw_df=latest_or_selected,
            silver_matches_df=silver_matches,
        )

        validate_silver_goals(silver_goals, silver_matches)
        write_silver_goals(silver_goals, config)

        print("silver.goals successfully written.")
        print(f"rows={silver_goals.count()}")
        print(f"distinct_matches={silver_goals.select('match_id').distinct().count()}")
        print(f"distinct_seasons={silver_goals.select('season').distinct().count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
