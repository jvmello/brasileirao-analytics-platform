from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from jobs.common import (
    build_spark_session,
    filter_latest_load,
    get_silver_prefix,
    normalize_string,
    read_bronze_layer,
)
from jobs.config import AppConfig
from jobs.silver.validation import (
    raise_if_critical_failures,
    validate_silver_matches,
    write_invalid_records,
)


def transform_matches(df: DataFrame) -> DataFrame:
    renamed = df.select(
        F.col("ID").cast(LongType()).alias("match_id"),
        F.col("rodada").cast(IntegerType()).alias("round"),
        normalize_string("data").alias("match_date_raw"),
        normalize_string("hora").alias("match_time"),
        normalize_string("mandante").alias("home_team"),
        normalize_string("visitante").alias("away_team"),
        normalize_string("formacao_mandante").alias("home_formation"),
        normalize_string("formacao_visitante").alias("away_formation"),
        normalize_string("tecnico_mandante").alias("home_coach"),
        normalize_string("tecnico_visitante").alias("away_coach"),
        normalize_string("vencedor").alias("winner"),
        normalize_string("arena").alias("stadium"),
        F.col("mandante_Placar").cast(IntegerType()).alias("home_score"),
        F.col("visitante_Placar").cast(IntegerType()).alias("away_score"),
        normalize_string("mandante_Estado").alias("home_state"),
        normalize_string("visitante_Estado").alias("away_state"),
        F.col("arrecadacao").cast(DoubleType()).alias("gross_revenue"),
        F.to_date(F.col("_load_date"), "yyyy-MM-dd").alias("ingestion_date"),
        F.col("_source_file").alias("source_file"),
    )

    transformed = (
        renamed.withColumn(
            "match_date",
            F.coalesce(
                F.to_date("match_date_raw", "dd/MM/yyyy"),
                F.to_date("match_date_raw", "yyyy-MM-dd"),
            ),
        )
        .withColumn(
            "match_datetime",
            F.coalesce(
                F.to_timestamp(
                    F.concat_ws(" ", F.col("match_date_raw"), F.col("match_time")),
                    "dd/MM/yyyy HH:mm",
                ),
                F.to_timestamp(
                    F.concat_ws(" ", F.col("match_date_raw"), F.col("match_time")),
                    "yyyy-MM-dd HH:mm",
                ),
                F.to_timestamp(
                    F.concat_ws(" ", F.col("match_date_raw"), F.col("match_time")),
                    "dd/MM/yyyy HH:mm:ss",
                ),
                F.to_timestamp(
                    F.concat_ws(" ", F.col("match_date_raw"), F.col("match_time")),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            ),
        )
        .withColumn("season", F.year("match_date"))
        .withColumn("total_goals", F.col("home_score") + F.col("away_score"))
        .withColumn("is_draw", F.col("home_score") == F.col("away_score"))
        .withColumn(
            "home_result",
            F.when(F.col("home_score") > F.col("away_score"), F.lit("win"))
            .when(F.col("home_score") < F.col("away_score"), F.lit("loss"))
            .otherwise(F.lit("draw")),
        )
        .withColumn(
            "away_result",
            F.when(F.col("away_score") > F.col("home_score"), F.lit("win"))
            .when(F.col("away_score") < F.col("home_score"), F.lit("loss"))
            .otherwise(F.lit("draw")),
        )
        .withColumn(
            "winner_normalized",
            F.when(F.col("winner") == "-", F.lit(None)).otherwise(F.col("winner")),
        )
        .drop("match_date_raw")
    )

    return transformed


def validate_matches(df: DataFrame) -> None:
    duplicate_match_ids = (
        df.groupBy("match_id").count().filter(F.col("count") > 1).count()
    )

    null_match_ids = df.filter(F.col("match_id").isNull()).count()
    null_match_dates = df.filter(F.col("match_date").isNull()).count()
    null_seasons = df.filter(F.col("season").isNull()).count()

    same_teams = df.filter(F.col("home_team") == F.col("away_team")).count()

    negative_scores = df.filter(
        (F.col("home_score") < 0) | (F.col("away_score") < 0)
    ).count()

    invalid_draw_winner = df.filter(
        (F.col("is_draw") == True)
        & F.col("winner").isNotNull()
        & (F.col("winner") != "-")
    ).count()

    invalid_non_draw_winner = df.filter(
        (F.col("is_draw") == False)
        & (
            F.col("winner").isNull()
            | (
                (F.col("winner") != F.col("home_team"))
                & (F.col("winner") != F.col("away_team"))
            )
        )
    ).count()

    errors = []

    if duplicate_match_ids > 0:
        errors.append(f"duplicate match_id rows found: {duplicate_match_ids}")

    if null_match_ids > 0:
        errors.append(f"null match_id rows found: {null_match_ids}")

    if null_match_dates > 0:
        errors.append(f"null match_date rows found: {null_match_dates}")

    if null_seasons > 0:
        errors.append(f"null season rows found: {null_seasons}")

    if same_teams > 0:
        errors.append(f"rows with home_team == away_team: {same_teams}")

    if negative_scores > 0:
        errors.append(f"rows with negative scores: {negative_scores}")

    if invalid_draw_winner > 0:
        errors.append(f"draw matches with invalid winner value: {invalid_draw_winner}")

    if invalid_non_draw_winner > 0:
        errors.append(
            f"non-draw matches with invalid winner value: {invalid_non_draw_winner}"
        )

    if errors:
        raise ValueError("silver.matches validation failed:\n- " + "\n- ".join(errors))


def write_silver_matches(df: DataFrame, config: AppConfig) -> None:
    silver_prefix = get_silver_prefix(config)
    silver_path = f"s3a://{config.bucket_name}/{silver_prefix}/matches/"

    (df.write.mode("overwrite").partitionBy("season").parquet(silver_path))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build silver.matches from bronze raw matches."
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
    spark = build_spark_session("silver-matches", config)

    try:
        bronze_matches = read_bronze_layer(spark=spark, config=config, source="matches")

        if bronze_matches.limit(1).count() == 0:
            raise ValueError("No bronze matches files found.")

        latest_or_selected = filter_latest_load(bronze_matches, args.load_date)
        silver_matches = transform_matches(latest_or_selected)

        validate_matches(silver_matches)
        write_silver_matches(silver_matches, config)

        row_count = silver_matches.count()
        season_count = silver_matches.select("season").distinct().count()

        print(f"silver.matches successfully written.")
        print(f"rows={row_count}")
        print(f"distinct_seasons={season_count}")

        report = validate_silver_matches(silver_matches)
        raise_if_critical_failures(report)

        for check in report["checks"]:
            print(
                f"[{check['severity'].upper()}] "
                f"{check['check_name']} -> invalid_count={check['invalid_count']}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
