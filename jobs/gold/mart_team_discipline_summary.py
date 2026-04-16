from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from jobs.config import AppConfig
from jobs.common import build_spark_session


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_fact_cards(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_cards/"
    return spark.read.parquet(path)


def read_fact_team_match_statistics(spark: SparkSession, config: AppConfig) -> DataFrame:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/fact_team_match_statistics/"
    return spark.read.parquet(path)


def transform_team_discipline_summary(cards_df: DataFrame, stats_df: DataFrame) -> DataFrame:
    card_agg = (
        cards_df.groupBy("season", "team")
        .agg(
            F.sum(F.when(F.col("card_type") == "yellow", 1).otherwise(0)).alias("yellow_cards_events"),
            F.sum(F.when(F.col("card_type") == "red", 1).otherwise(0)).alias("red_cards_events"),
            F.sum("card_weight").alias("discipline_points"),
        )
    )

    match_agg = (
        stats_df.groupBy("season", "team")
        .agg(
            F.count("*").alias("matches"),
            F.sum("fouls").alias("fouls"),
            F.avg("fouls").alias("avg_fouls"),
            F.avg("yellow_cards").alias("avg_yellow_cards_boxscore"),
            F.avg("red_cards").alias("avg_red_cards_boxscore"),
        )
    )

    return (
        match_agg.alias("m")
        .join(card_agg.alias("c"), on=["season", "team"], how="left")
        .fillna({
            "yellow_cards_events": 0,
            "red_cards_events": 0,
            "discipline_points": 0,
        })
        .withColumn(
            "cards_per_match",
            F.when(F.col("matches") == 0, F.lit(None))
             .otherwise((F.col("yellow_cards_events") + F.col("red_cards_events")) / F.col("matches"))
        )
    )


def write_team_discipline_summary(df: DataFrame, config: AppConfig) -> None:
    gold_prefix = get_gold_prefix(config)
    path = f"s3a://{config.bucket_name}/{gold_prefix}/marts/team_discipline_summary/"
    (
        df.write
        .mode("overwrite")
        .partitionBy("season")
        .parquet(path)
    )


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("mart-team-discipline-summary", config)

    try:
        fact_cards = read_fact_cards(spark, config)
        fact_stats = read_fact_team_match_statistics(spark, config)
        mart = transform_team_discipline_summary(fact_cards, fact_stats)
        write_team_discipline_summary(mart, config)

        print("gold.marts.team_discipline_summary successfully written.")
        print(f"rows={mart.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()