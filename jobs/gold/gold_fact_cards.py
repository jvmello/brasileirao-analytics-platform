from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(
        config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")
    ).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_cards(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/cards/"
    return spark.read.parquet(path)


def read_dim_team(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/dim_team/"
    return spark.read.parquet(path)


def read_dim_player(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/dim_player/"
    return spark.read.parquet(path)


def transform_fact_cards(
    silver_cards_df: DataFrame,
    dim_team_df: DataFrame,
    dim_player_df: DataFrame,
) -> DataFrame:
    teams = dim_team_df.select(
        F.col("id").alias("team_id"),
        F.col("team_name_raw"),
    )

    players = dim_player_df.select(
        F.col("id").alias("player_id"),
        F.col("player_name_raw"),
    )

    return (
        silver_cards_df.alias("c")
        .join(
            teams.alias("t"),
            F.trim(F.col("c.team")) == F.col("t.team_name_raw"),
            "left",
        )
        .join(
            players.alias("p"),
            F.trim(F.col("c.player")) == F.col("p.player_name_raw"),
            "left",
        )
        .select(
            F.col("c.card_id"),
            F.col("c.match_id"),
            F.col("c.round"),
            F.col("c.season"),
            F.col("c.match_date"),
            F.col("t.team_id"),
            F.col("p.player_id"),

            # Temporary raw columns. Remove later after API/marts are adjusted.
            F.col("c.team"),
            F.col("c.player"),

            F.col("c.shirt_number"),
            F.col("c.position"),
            F.col("c.minute_raw"),
            F.col("c.minute_base"),
            F.col("c.stoppage_minute"),
            F.col("c.minute"),
            F.col("c.minute_bucket"),
            F.col("c.card_type"),
            F.col("c.card_weight"),
            F.col("c.is_home_team_card"),
        )
    )


def validate_fact_cards(df: DataFrame) -> None:
    checks = []

    checks.append(("null_card_id", df.filter(F.col("card_id").isNull()).count()))
    checks.append(("null_match_id", df.filter(F.col("match_id").isNull()).count()))
    checks.append(("null_team_id", df.filter(F.col("team_id").isNull()).count()))
    checks.append(
        (
            "unmapped_player_id",
            df.filter(F.col("player").isNotNull() & F.col("player_id").isNull()).count(),
        )
    )
    checks.append(("null_season", df.filter(F.col("season").isNull()).count()))
    checks.append(
        (
            "duplicate_card_id",
            df.groupBy("card_id").count().filter(F.col("count") > 1).count(),
        )
    )
    checks.append(
        (
            "invalid_card_type",
            df.filter(~F.col("card_type").isin("yellow", "red")).count(),
        )
    )
    checks.append(("negative_minute", df.filter(F.col("minute") < 0).count()))

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["gold.fact_cards validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        raise ValueError("\n".join(lines))


def write_fact_cards(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_cards/"

    df.write.mode("overwrite").partitionBy("season").parquet(path)


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-fact-cards", config)

    try:
        silver_cards = read_silver_cards(spark, config)
        dim_team = read_dim_team(spark, config)
        dim_player = read_dim_player(spark, config)

        if silver_cards.limit(1).count() == 0:
            raise ValueError("No silver cards files found.")

        fact_cards = transform_fact_cards(
            silver_cards_df=silver_cards,
            dim_team_df=dim_team,
            dim_player_df=dim_player,
        )

        validate_fact_cards(fact_cards)
        write_fact_cards(fact_cards, config)

        print("gold.fact_cards successfully written.")
        print(f"rows={fact_cards.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()