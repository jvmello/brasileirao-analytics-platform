from __future__ import annotations

import argparse
import os
import unicodedata

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType

from jobs.config import AppConfig
from jobs.silver.common import build_spark_session

from jobs.silver.common import get_silver_prefix, normalize_string, read_bronze_layer, filter_latest_load

def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return (
        value.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
    )


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
    cleaned = (
        F.regexp_replace(F.trim(F.col(column_name).cast("string")), ",", ".")
    )
    cleaned = F.regexp_replace(cleaned, r"[^0-9\.\-]", "")
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned.cast(DoubleType()))


def parse_percentage(column_name: str) -> F.Column:
    cleaned = F.trim(F.col(column_name).cast("string"))
    cleaned = F.regexp_replace(cleaned, "%", "")
    cleaned = F.regexp_replace(cleaned, ",", ".")
    cleaned = F.regexp_replace(cleaned, r"[^0-9\.\-]", "")
    return F.when(cleaned == "", F.lit(None)).otherwise(cleaned.cast(DoubleType()))


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    silver_matches_path = f"s3a://{config.bucket_name}/{silver_prefix}/matches/"

    return (
        spark.read
        .parquet(silver_matches_path)
        .select(
            "match_id",
            "season",
            "match_date",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "home_result",
            "away_result",
        )
    )


def transform_match_statistics(raw_df: DataFrame, silver_matches_df: DataFrame) -> DataFrame:
    match_id_col = resolve_column(raw_df, "partida_id", "partida_ID", "match_id")
    round_col = resolve_column(raw_df, "rodada", "round")
    team_col = resolve_column(raw_df, "clube", "team")
    shots_col = resolve_column(raw_df, "chutes", "shots")
    shots_on_target_col = resolve_column(
        raw_df,
        "chutes_no_alvo",
        "chutes a gol",
        "chutes_a_gol",
        "shots_on_target",
    )
    possession_col = resolve_column(raw_df, "posse_de_bola", "posse de bola", "possession")
    passes_col = resolve_column(raw_df, "passes")
    pass_accuracy_col = resolve_column(
        raw_df,
        "precisao_passes",
        "precisão_passes",
        "precisao de passes",
        "pass_accuracy",
    )
    fouls_col = resolve_column(raw_df, "faltas", "fouls")
    yellow_cards_col = resolve_column(raw_df, "cartao_amarelo", "cartão_amarelo", "yellow_cards")
    red_cards_col = resolve_column(raw_df, "cartao_vermelho", "cartão_vermelho", "red_cards")
    offsides_col = resolve_column(raw_df, "impedimentos", "offsides")
    corners_col = resolve_column(raw_df, "escanteios", "corners")

    canonical = raw_df.select(
        F.col(match_id_col).cast(LongType()).alias("match_id"),
        F.col(round_col).cast(IntegerType()).alias("round"),
        normalize_string(team_col).alias("team"),
        parse_numeric(shots_col).cast(IntegerType()).alias("shots"),
        parse_numeric(shots_on_target_col).cast(IntegerType()).alias("shots_on_target"),
        parse_percentage(possession_col).alias("possession"),
        parse_numeric(passes_col).cast(IntegerType()).alias("passes"),
        parse_percentage(pass_accuracy_col).alias("pass_accuracy"),
        parse_numeric(fouls_col).cast(IntegerType()).alias("fouls"),
        parse_numeric(yellow_cards_col).cast(IntegerType()).alias("yellow_cards"),
        parse_numeric(red_cards_col).cast(IntegerType()).alias("red_cards"),
        parse_numeric(offsides_col).cast(IntegerType()).alias("offsides"),
        parse_numeric(corners_col).cast(IntegerType()).alias("corners"),
        F.to_date(F.col("_load_date"), "yyyy-MM-dd").alias("ingestion_date"),
        F.col("_source_file").alias("source_file"),
    )

    enriched = (
        canonical.alias("stats")
        .join(
            silver_matches_df.alias("matches"),
            on="match_id",
            how="left",
        )
        .withColumn(
            "is_home_team",
            F.when(F.col("team") == F.col("home_team"), F.lit(True))
             .when(F.col("team") == F.col("away_team"), F.lit(False))
             .otherwise(F.lit(None).cast(BooleanType()))
        )
        .withColumn(
            "opponent_team",
            F.when(F.col("is_home_team") == True, F.col("away_team"))
             .when(F.col("is_home_team") == False, F.col("home_team"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "goals_scored",
            F.when(F.col("is_home_team") == True, F.col("home_score"))
             .when(F.col("is_home_team") == False, F.col("away_score"))
             .otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn(
            "goals_conceded",
            F.when(F.col("is_home_team") == True, F.col("away_score"))
             .when(F.col("is_home_team") == False, F.col("home_score"))
             .otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn(
            "match_result",
            F.when(F.col("goals_scored") > F.col("goals_conceded"), F.lit("win"))
             .when(F.col("goals_scored") < F.col("goals_conceded"), F.lit("loss"))
             .when(
                 F.col("goals_scored").isNotNull() & F.col("goals_conceded").isNotNull(),
                 F.lit("draw"),
             )
             .otherwise(F.lit(None))
        )
        .withColumn(
            "shot_accuracy",
            F.when(
                (F.col("shots").isNull()) | (F.col("shots") == 0),
                F.lit(None),
            ).otherwise(F.col("shots_on_target") / F.col("shots"))
        )
        .withColumn(
            "scoring_efficiency",
            F.when(
                (F.col("shots").isNull()) | (F.col("shots") == 0),
                F.lit(None),
            ).otherwise(F.col("goals_scored") / F.col("shots"))
        )
        .select(
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
            "ingestion_date",
            "source_file",
        )
    )

    return enriched


def validate_silver_match_statistics(df: DataFrame) -> None:
    checks = []

    checks.append((
        "null_match_id",
        df.filter(F.col("match_id").isNull()).count(),
    ))

    checks.append((
        "null_team",
        df.filter(F.col("team").isNull()).count(),
    ))

    checks.append((
        "null_season",
        df.filter(F.col("season").isNull()).count(),
    ))

    checks.append((
        "invalid_team_for_match",
        df.filter(F.col("is_home_team").isNull()).count(),
    ))

    checks.append((
        "duplicate_match_team_rows",
        df.groupBy("match_id", "team").count().filter(F.col("count") > 1).count(),
    ))

    checks.append((
        "rows_per_match_not_equal_2",
        df.groupBy("match_id").count().filter(F.col("count") != 2).count(),
    ))

    checks.append((
        "shots_on_target_greater_than_shots",
        df.filter(
            F.col("shots").isNotNull()
            & F.col("shots_on_target").isNotNull()
            & (F.col("shots_on_target") > F.col("shots"))
        ).count(),
    ))

    checks.append((
        "invalid_possession_range",
        df.filter(
            F.col("possession").isNotNull()
            & ((F.col("possession") < 0) | (F.col("possession") > 100))
        ).count(),
    ))

    checks.append((
        "invalid_pass_accuracy_range",
        df.filter(
            F.col("pass_accuracy").isNotNull()
            & ((F.col("pass_accuracy") < 0) | (F.col("pass_accuracy") > 100))
        ).count(),
    ))

    checks.append((
        "negative_numeric_stats",
        df.filter(
            (F.col("shots") < 0)
            | (F.col("shots_on_target") < 0)
            | (F.col("passes") < 0)
            | (F.col("fouls") < 0)
            | (F.col("yellow_cards") < 0)
            | (F.col("red_cards") < 0)
            | (F.col("offsides") < 0)
            | (F.col("corners") < 0)
        ).count(),
    ))

    checks.append((
        "goals_scored_mismatch_vs_result",
        df.filter(
            F.col("match_result").isNotNull()
            & (
                F.when(F.col("goals_scored") > F.col("goals_conceded"), F.lit("win"))
                 .when(F.col("goals_scored") < F.col("goals_conceded"), F.lit("loss"))
                 .otherwise(F.lit("draw"))
                != F.col("match_result")
            )
        ).count(),
    ))

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["silver.match_statistics validation failed:"]
        lines.extend([f"- {name}: {count} invalid rows/groups" for name, count in failing])
        raise ValueError("\n".join(lines))


def write_silver_match_statistics(df: DataFrame, config: AppConfig) -> None:
    silver_prefix = get_silver_prefix(config)
    silver_path = f"s3a://{config.bucket_name}/{silver_prefix}/match_statistics/"

    (
        df.write
        .mode("overwrite")
        .partitionBy("season")
        .parquet(silver_path)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver.match_statistics from bronze raw match_statistics.")
    parser.add_argument(
        "--load-date",
        required=False,
        help="Optional load_date to process in YYYY-MM-DD format. Defaults to latest bronze load.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig()
    spark = build_spark_session("silver-match-statistics", config)

    try:
        bronze_stats = read_bronze_layer(spark, config, source="match_statistics")

        if bronze_stats.limit(1).count() == 0:
            raise ValueError("No bronze match_statistics files found.")

        latest_or_selected = filter_latest_load(bronze_stats, args.load_date)
        silver_matches = read_silver_matches(spark, config)

        silver_match_statistics = transform_match_statistics(
            raw_df=latest_or_selected,
            silver_matches_df=silver_matches,
        )

        validate_silver_match_statistics(silver_match_statistics)
        write_silver_match_statistics(silver_match_statistics, config)

        print("silver.match_statistics successfully written.")
        print(f"rows={silver_match_statistics.count()}")
        print(f"distinct_matches={silver_match_statistics.select('match_id').distinct().count()}")
        print(f"distinct_seasons={silver_match_statistics.select('season').distinct().count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()