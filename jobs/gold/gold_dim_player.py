from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(
        config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")
    ).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_goals(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/goals/"
    return spark.read.parquet(path)


def read_silver_cards(spark: SparkSession, config: AppConfig) -> DataFrame:
    silver_prefix = get_silver_prefix(config)
    path = f"s3a://{config.bucket_name}/{silver_prefix}/cards/"
    return spark.read.parquet(path)


def build_player_key(column_name: str) -> F.Column:
    cleaned = F.lower(F.trim(F.col(column_name)))
    cleaned = F.regexp_replace(cleaned, r"\s+", " ")
    cleaned = F.regexp_replace(cleaned, r"[^a-zA-Z0-9À-ÿ]+", "_")
    cleaned = F.regexp_replace(cleaned, r"(^_+|_+$)", "")

    return cleaned


def transform_dim_player(
    silver_goals_df: DataFrame,
    silver_cards_df: DataFrame,
) -> DataFrame:
    goal_players = silver_goals_df.select(
        F.trim(F.col("player")).alias("player_name_raw")
    )

    card_players = silver_cards_df.select(
        F.trim(F.col("player")).alias("player_name_raw")
    )

    players = (
        goal_players
        .unionByName(card_players)
        .filter(F.col("player_name_raw").isNotNull())
        .filter(F.col("player_name_raw") != "")
        .dropDuplicates(["player_name_raw"])
    )

    window = Window.orderBy("player_name_raw")

    return (
        players
        .withColumn("id", F.dense_rank().over(window).cast("long"))
        .withColumn("player_name", F.col("player_name_raw"))
        .withColumn("player_key", build_player_key("player_name_raw"))
        .select(
            "id",
            "player_name_raw",
            "player_name",
            "player_key",
        )
    )


def validate_dim_player(df: DataFrame) -> None:
    checks = []

    checks.append(("null_id", df.filter(F.col("id").isNull()).count()))
    checks.append(
        ("null_player_name_raw", df.filter(F.col("player_name_raw").isNull()).count())
    )
    checks.append(
        (
            "duplicate_id",
            df.groupBy("id").count().filter(F.col("count") > 1).count(),
        )
    )
    checks.append(
        (
            "duplicate_player_name_raw",
            df.groupBy("player_name_raw").count().filter(F.col("count") > 1).count(),
        )
    )

    failing = [(name, count) for name, count in checks if count > 0]

    if failing:
        lines = ["gold.dim_player validation failed:"]
        lines.extend(
            [f"- {name}: {count} invalid rows/groups" for name, count in failing]
        )
        raise ValueError("\n".join(lines))


def write_dim_player(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/dim_player/"

    df.write.mode("overwrite").parquet(path)


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-dim-player", config)

    try:
        silver_goals = read_silver_goals(spark, config)
        silver_cards = read_silver_cards(spark, config)

        if silver_goals.limit(1).count() == 0 and silver_cards.limit(1).count() == 0:
            raise ValueError("No silver goals or cards files found.")

        dim_player = transform_dim_player(silver_goals, silver_cards)

        validate_dim_player(dim_player)
        write_dim_player(dim_player, config)

        print("gold.dim_player successfully written.")
        print(f"rows={dim_player.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()