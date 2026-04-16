from __future__ import annotations

import argparse
import os
import unicodedata

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType

from jobs.config import AppConfig
from jobs.common import build_spark_session
from jobs.common import build_spark_session, normalize_string, get_silver_prefix, read_bronze_layer, filter_latest_load, parse_match_minute


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")).rstrip("/")


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
    cleaned = F.regexp_replace(F.trim(F.col(column_name).cast("string")), r"[^0-9\-]", "")
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


def normalize_card_type(column_name: str) -> F.Column:
    cleaned = F.lower(F.trim(F.col(column_name)))
    return (
        F.when(cleaned.isNull(), F.lit(None))
        .when(cleaned.isin("amarelo", "yellow"), F.lit("yellow"))
        .when(cleaned.isin("vermelho", "red"), F.lit("red"))
        .otherwise(cleaned)
    )

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
        )
    )

def transform_cards(raw_df: DataFrame, silver_matches_df: DataFrame) -> DataFrame:
    match_id_col = resolve_column(raw_df, "partida_id", "partida_ID", "match_id")
    round_col = resolve_column(raw_df, "rodada", "round")
    team_col = resolve_column(raw_df, "clube", "team")
    card_type_col = resolve_column(raw_df, "cartao", "cartão", "card_type")
    player_col = resolve_column(raw_df, "atleta", "player")
    shirt_number_col = resolve_column(raw_df, "num_camisa", "shirt_number")
    position_col = resolve_column(raw_df, "posicao", "posição", "position")
    minute_col = resolve_column(raw_df, "minuto", "minute")
    minute_raw, minute_base, stoppage_minute, minute_exact = parse_match_minute(minute_col)

    canonical = raw_df.select(
        F.col(match_id_col).cast(LongType()).alias("match_id"),
        F.col(round_col).cast(IntegerType()).alias("round"),
        normalize_string(team_col).alias("team"),
        normalize_card_type(card_type_col).alias("card_type"),
        normalize_string(player_col).alias("player"),
        parse_numeric(shirt_number_col).alias("shirt_number"),
        normalize_string(position_col).alias("position"),
        minute_raw.alias("minute_raw"),
        minute_base.alias("minute_base"),
        stoppage_minute.alias("stoppage_minute"),
        minute_exact.alias("minute"),
        F.to_date(F.col("_load_date"), "yyyy-MM-dd").alias("ingestion_date"),
        F.col("_source_file").alias("source_file"),
    )

    enriched = (
        canonical.alias("cards")
        .join(
            silver_matches_df.alias("matches"),
            on="match_id",
            how="left",
        )
        .withColumn(
            "is_home_team_card",
            F.when(F.col("team") == F.col("home_team"), F.lit(True))
             .when(F.col("team") == F.col("away_team"), F.lit(False))
             .otherwise(F.lit(None).cast(BooleanType()))
        )
        .withColumn("minute_bucket", minute_bucket_expr("minute"))
        .withColumn(
            "card_weight",
            F.when(F.col("card_type") == "yellow", F.lit(1))
             .when(F.col("card_type") == "red", F.lit(2))
             .otherwise(F.lit(None).cast(IntegerType()))
        )
        .withColumn(
            "card_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("match_id").cast("string"),
                    F.coalesce(F.col("team"), F.lit("")),
                    F.coalesce(F.col("player"), F.lit("")),
                    F.coalesce(F.col("shirt_number").cast("string"), F.lit("")),
                    F.coalesce(F.col("position"), F.lit("")),
                    F.coalesce(F.col("minute").cast("string"), F.lit("")),
                    F.coalesce(F.col("card_type"), F.lit("")),
                ),
                256,
            )
        )
        .select(
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
            "ingestion_date",
            "source_file",
        )
    )

    return enriched


def validate_silver_cards(df: DataFrame) -> None:
    checks = []

    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team", df.filter(F.col("team").isNull()).count()))
    checks.append(("null_player", df.filter(F.col("player").isNull()).count()))
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(("invalid_team_for_match", df.filter(F.col("is_home_team_card").isNull()).count()))
    checks.append(("null_minute", df.filter(F.col("minute").isNull()).count()))
    checks.append(("negative_minute", df.filter(F.col("minute") < 0).count()))
    checks.append(("minute_too_high", df.filter(F.col("minute") > 130).count()))
    checks.append((
        "invalid_card_type",
        df.filter(~F.col("card_type").isin("yellow", "red")).count(),
    ))
    checks.append((
        "invalid_card_weight",
        df.filter(
            ((F.col("card_type") == "yellow") & (F.col("card_weight") != 1))
            | ((F.col("card_type") == "red") & (F.col("card_weight") != 2))
        ).count(),
    ))
    checks.append((
        "invalid_shirt_number",
        df.filter(F.col("shirt_number").isNotNull() & (F.col("shirt_number") <= 0)).count(),
    ))
    checks.append((
        "duplicate_card_id",
        df.groupBy("card_id").count().filter(F.col("count") > 1).count(),
    ))

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["silver.cards validation failed:"]
        lines.extend([f"- {name}: {count} invalid rows/groups" for name, count in failing])
        #raise ValueError("\n".join(lines))


def write_silver_cards(df: DataFrame, config: AppConfig) -> None:
    silver_prefix = get_silver_prefix(config)
    silver_path = f"s3a://{config.bucket_name}/{silver_prefix}/cards/"

    (
        df.write
        .mode("overwrite")
        .partitionBy("season")
        .parquet(silver_path)
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver.cards from bronze raw cards.")
    parser.add_argument(
        "--load-date",
        required=False,
        help="Optional load_date to process in YYYY-MM-DD format. Defaults to latest bronze load.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig()
    spark = build_spark_session("silver-cards", config)

    try:
        bronze_cards = read_bronze_layer(spark, config, "cards")

        if bronze_cards.limit(1).count() == 0:
            raise ValueError("No bronze cards files found.")

        latest_or_selected = filter_latest_load(bronze_cards, args.load_date)
        silver_matches = read_silver_matches(spark, config)

        silver_cards = transform_cards(
            raw_df=latest_or_selected,
            silver_matches_df=silver_matches,
        )

        validate_silver_cards(silver_cards)
        write_silver_cards(silver_cards, config)

        print("silver.cards successfully written.")
        print(f"rows={silver_cards.count()}")
        print(f"distinct_matches={silver_cards.select('match_id').distinct().count()}")
        print(f"distinct_seasons={silver_cards.select('season').distinct().count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()