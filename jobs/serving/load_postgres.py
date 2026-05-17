from __future__ import annotations

import os

import psycopg2
from pyspark.sql import SparkSession

from jobs.common import build_spark_session
from jobs.config import AppConfig

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "brasileirao")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

BASE_PATH = "s3a://brasileirao/gold"
SCHEMA_NAME = "analytics"


def reset_schema() -> None:
    print(f"Resetting schema: {SCHEMA_NAME}")

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_NAME} CASCADE;")
            cur.execute(f"CREATE SCHEMA {SCHEMA_NAME};")

    finally:
        conn.close()


def write_table(df, table_name: str, mode: str = "overwrite") -> None:
    print(f"Writing table: {SCHEMA_NAME}.{table_name}")

    (
        df.write.format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"{SCHEMA_NAME}.{table_name}")
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .mode(mode)
        .save()
    )


def load_dim_tables(spark: SparkSession) -> None:
    print("Loading DIM tables...")

    dim_team = spark.read.parquet(f"{BASE_PATH}/dim_team")
    dim_player = spark.read.parquet(f"{BASE_PATH}/dim_player")
    dim_stadium = spark.read.parquet(f"{BASE_PATH}/dim_stadium")

    write_table(dim_team, "dim_team")
    write_table(dim_player, "dim_player")
    write_table(dim_stadium, "dim_stadium")


def load_fact_tables(spark: SparkSession) -> None:
    print("Loading FACT tables...")

    fact_matches = spark.read.parquet(f"{BASE_PATH}/fact_matches")
    fact_goals = spark.read.parquet(f"{BASE_PATH}/fact_goals")
    fact_cards = spark.read.parquet(f"{BASE_PATH}/fact_cards")
    fact_team_match_statistics = spark.read.parquet(
        f"{BASE_PATH}/fact_team_match_statistics"
    )

    write_table(fact_matches, "fact_matches")
    write_table(fact_goals, "fact_goals")
    write_table(fact_cards, "fact_cards")
    write_table(fact_team_match_statistics, "fact_team_match_statistics")


def load_marts(spark: SparkSession) -> None:
    print("Loading MART tables...")

    team_summary = spark.read.parquet(f"{BASE_PATH}/marts/team_season_summary")
    home_away = spark.read.parquet(f"{BASE_PATH}/marts/team_home_away_summary")
    scorers = spark.read.parquet(f"{BASE_PATH}/marts/top_scorers_by_season")
    discipline = spark.read.parquet(f"{BASE_PATH}/marts/team_discipline_summary")

    write_table(team_summary, "team_season_summary")
    write_table(home_away, "team_home_away_summary")
    write_table(scorers, "top_scorers_by_season")
    write_table(discipline, "team_discipline_summary")


def create_constraints() -> None:
    print("Creating constraints...")

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    try:
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("""
                ALTER TABLE analytics.dim_team
                ADD CONSTRAINT pk_dim_team PRIMARY KEY (id);
            """)

            cur.execute("""
                ALTER TABLE analytics.dim_player
                ADD CONSTRAINT pk_dim_player PRIMARY KEY (id);
            """)

            cur.execute("""
                ALTER TABLE analytics.dim_stadium
                ADD CONSTRAINT pk_dim_stadium PRIMARY KEY (id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_matches
                ADD CONSTRAINT pk_fact_matches PRIMARY KEY (match_id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_matches
                ADD CONSTRAINT fk_fact_matches_home_team
                FOREIGN KEY (home_team_id)
                REFERENCES analytics.dim_team(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_matches
                ADD CONSTRAINT fk_fact_matches_away_team
                FOREIGN KEY (away_team_id)
                REFERENCES analytics.dim_team(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_matches
                ADD CONSTRAINT fk_fact_matches_stadium
                FOREIGN KEY (stadium_id)
                REFERENCES analytics.dim_stadium(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_goals
                ADD CONSTRAINT fk_fact_goals_match
                FOREIGN KEY (match_id)
                REFERENCES analytics.fact_matches(match_id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_goals
                ADD CONSTRAINT fk_fact_goals_team
                FOREIGN KEY (team_id)
                REFERENCES analytics.dim_team(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_goals
                ADD CONSTRAINT fk_fact_goals_player
                FOREIGN KEY (player_id)
                REFERENCES analytics.dim_player(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_cards
                ADD CONSTRAINT fk_fact_cards_match
                FOREIGN KEY (match_id)
                REFERENCES analytics.fact_matches(match_id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_cards
                ADD CONSTRAINT fk_fact_cards_team
                FOREIGN KEY (team_id)
                REFERENCES analytics.dim_team(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_cards
                ADD CONSTRAINT fk_fact_cards_player
                FOREIGN KEY (player_id)
                REFERENCES analytics.dim_player(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_team_match_statistics
                ADD CONSTRAINT fk_fact_team_match_statistics_match
                FOREIGN KEY (match_id)
                REFERENCES analytics.fact_matches(match_id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_team_match_statistics
                ADD CONSTRAINT fk_fact_team_match_statistics_team
                FOREIGN KEY (team_id)
                REFERENCES analytics.dim_team(id);
            """)

            cur.execute("""
                ALTER TABLE analytics.fact_team_match_statistics
                ADD CONSTRAINT fk_fact_team_match_statistics_opponent_team
                FOREIGN KEY (opponent_team_id)
                REFERENCES analytics.dim_team(id);
            """)

    finally:
        conn.close()


def main() -> None:
    config = AppConfig()
    spark = build_spark_session("postgres", config)

    try:
        reset_schema()

        load_dim_tables(spark)
        load_fact_tables(spark)

        # Keep this enabled only if marts were already updated to the new IDs/model.
        load_marts(spark)

        create_constraints()

        print("PostgreSQL load completed successfully")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
