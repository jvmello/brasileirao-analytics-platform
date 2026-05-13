from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from jobs.common import build_spark_session
from jobs.config import AppConfig


def get_silver_prefix(config: AppConfig) -> str:
    return getattr(config, "silver_prefix", os.getenv("SILVER_PREFIX", "silver")).rstrip("/")


def get_gold_prefix(config: AppConfig) -> str:
    return getattr(config, "gold_prefix", os.getenv("GOLD_PREFIX", "gold")).rstrip("/")


def read_silver_matches(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"s3a://{config.bucket_name}/{get_silver_prefix(config)}/matches/"
    return spark.read.parquet(path)


def read_team_mapping(spark: SparkSession, config: AppConfig) -> DataFrame:
    path = f"{config.seeds_path.rstrip('/')}/team_mapping.csv"

    return (
        spark.read
        .option("header", "true")
        .csv(path)
        .select(
            F.trim(F.col("team_name_raw")).alias("team_name_raw"),
            F.trim(F.col("team_name")).alias("mapped_team_name"),
            F.trim(F.col("state")).alias("mapped_state"),
        )
    )


def transform_dim_team(matches_df: DataFrame, team_mapping_df: DataFrame) -> DataFrame:
    home_teams = matches_df.select(
        F.trim(F.col("home_team")).alias("team_name_raw"),
        F.trim(F.col("home_state")).alias("state_raw"),
    )

    away_teams = matches_df.select(
        F.trim(F.col("away_team")).alias("team_name_raw"),
        F.trim(F.col("away_state")).alias("state_raw"),
    )

    teams = (
        home_teams
        .unionByName(away_teams)
        .filter(F.col("team_name_raw").isNotNull())
        .dropDuplicates(["team_name_raw"])
    )

    window = Window.orderBy("team_name_raw")

    return (
        teams
        .join(team_mapping_df, on="team_name_raw", how="left")
        .withColumn("team_name", F.coalesce(F.col("mapped_team_name"), F.col("team_name_raw")))
        .withColumn("state", F.coalesce(F.col("mapped_state"), F.col("state_raw")))
        .withColumn("id", F.dense_rank().over(window).cast("long"))
        .select(
            "id",
            "team_name_raw",
            "team_name",
            "state",
        )
    )


def write_dim_team(df: DataFrame, config: AppConfig) -> None:
    path = f"s3a://{config.bucket_name}/{get_gold_prefix(config)}/dim_team/"
    df.write.mode("overwrite").parquet(path)


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("gold-dim-team", config)

    try:
        silver_matches = read_silver_matches(spark, config)
        team_mapping = read_team_mapping(spark, config)

        dim_team = transform_dim_team(silver_matches, team_mapping)

        write_dim_team(dim_team, config)

        print("gold.dim_team successfully written.")
        print(f"rows={dim_team.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()