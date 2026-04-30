from pyspark.sql import SparkSession

from jobs.common import build_spark_session
from jobs.config import AppConfig

POSTGRES_URL = "jdbc:postgresql://localhost:5432/brasileirao"

POSTGRES_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

BASE_PATH = "s3a://brasileirao/gold"


def write_table(df, table_name, mode="overwrite"):
    print(f"Writing table: {table_name}")

    (
        df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", f"analytics.{table_name}")
        .option("user", POSTGRES_PROPERTIES["user"])
        .option("password", POSTGRES_PROPERTIES["password"])
        .option("driver", POSTGRES_PROPERTIES["driver"])
        .mode(mode)
        .save()
    )


def load_fact_tables(spark):
    print("Loading FACT tables...")

    fact_matches = spark.read.parquet(f"{BASE_PATH}/fact_matches")
    fact_goals = spark.read.parquet(f"{BASE_PATH}/fact_goals")
    fact_cards = spark.read.parquet(f"{BASE_PATH}/fact_cards")

    # Ordem importa por causa das FKs
    write_table(fact_matches, "fact_matches")
    write_table(fact_goals, "fact_goals")
    write_table(fact_cards, "fact_cards")


def load_marts(spark):
    print("Loading MART tables...")

    team_summary = spark.read.parquet(f"{BASE_PATH}/marts/team_season_summary")
    home_away = spark.read.parquet(f"{BASE_PATH}/marts/team_home_away_summary")
    scorers = spark.read.parquet(f"{BASE_PATH}/marts/top_scorers_by_season")
    discipline = spark.read.parquet(f"{BASE_PATH}/marts/team_discipline_summary")

    write_table(team_summary, "team_season_summary")
    write_table(home_away, "team_home_away_summary")
    write_table(scorers, "top_scorers_by_season")
    write_table(discipline, "team_discipline_summary")


def main():
    config = AppConfig()
    spark = build_spark_session("postgres", config)

    load_fact_tables(spark)
    load_marts(spark)

    print("PostgreSQL load completed successfully")


if __name__ == "__main__":
    main()