from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def _count(df: DataFrame) -> int:
    return df.count()


def _sample_records(df: DataFrame, limit: int = 20) -> list[dict]:
    return [row.asDict() for row in df.limit(limit).collect()]


def validate_silver_matches(df: DataFrame, sample_limit: int = 20) -> Dict:
    checks: List[Tuple[str, DataFrame, str]] = []

    checks.append((
        "null_match_id",
        df.filter(F.col("match_id").isNull()),
        "critical",
    ))

    checks.append((
        "duplicate_match_id",
        df.groupBy("match_id").count().filter(F.col("match_id").isNotNull() & (F.col("count") > 1)),
        "critical",
    ))

    checks.append((
        "null_match_date",
        df.filter(F.col("match_date").isNull()),
        "critical",
    ))

    checks.append((
        "null_season",
        df.filter(F.col("season").isNull()),
        "critical",
    ))

    checks.append((
        "null_home_team",
        df.filter(F.col("home_team").isNull()),
        "critical",
    ))

    checks.append((
        "null_away_team",
        df.filter(F.col("away_team").isNull()),
        "critical",
    ))

    checks.append((
        "same_home_away_team",
        df.filter(F.col("home_team") == F.col("away_team")),
        "critical",
    ))

    checks.append((
        "null_scores",
        df.filter(F.col("home_score").isNull() | F.col("away_score").isNull()),
        "critical",
    ))

    checks.append((
        "negative_scores",
        df.filter((F.col("home_score") < 0) | (F.col("away_score") < 0)),
        "critical",
    ))

    checks.append((
        "season_mismatch",
        df.filter(F.col("season") != F.year("match_date")),
        "critical",
    ))

    checks.append((
        "total_goals_mismatch",
        df.filter(F.col("total_goals") != (F.col("home_score") + F.col("away_score"))),
        "critical",
    ))

    checks.append((
        "is_draw_mismatch",
        df.filter(F.col("is_draw") != (F.col("home_score") == F.col("away_score"))),
        "critical",
    ))

    checks.append((
        "home_result_mismatch",
        df.filter(
            F.col("home_result") !=
            F.when(F.col("home_score") > F.col("away_score"), F.lit("win"))
             .when(F.col("home_score") < F.col("away_score"), F.lit("loss"))
             .otherwise(F.lit("draw"))
        ),
        "critical",
    ))

    checks.append((
        "away_result_mismatch",
        df.filter(
            F.col("away_result") !=
            F.when(F.col("away_score") > F.col("home_score"), F.lit("win"))
             .when(F.col("away_score") < F.col("home_score"), F.lit("loss"))
             .otherwise(F.lit("draw"))
        ),
        "critical",
    ))

    checks.append((
        "draw_with_winner",
        df.filter(F.col("is_draw") & F.col("winner_normalized").isNotNull()),
        "critical",
    ))

    checks.append((
        "non_draw_invalid_winner",
        df.filter(
            (~F.col("is_draw")) &
            (
                F.col("winner_normalized").isNull() |
                (
                    (F.col("winner_normalized") != F.col("home_team")) &
                    (F.col("winner_normalized") != F.col("away_team"))
                )
            )
        ),
        "critical",
    ))

    checks.append((
        "null_match_datetime",
        df.filter(F.col("match_datetime").isNull()),
        "warning",
    ))

    checks.append((
        "null_home_coach",
        df.filter(F.col("home_coach").isNull()),
        "warning",
    ))

    checks.append((
        "null_away_coach",
        df.filter(F.col("away_coach").isNull()),
        "warning",
    ))

    checks.append((
        "null_gross_revenue",
        df.filter(F.col("gross_revenue").isNull()),
        "warning",
    ))

    results = {
        "row_count": df.count(),
        "checks": [],
        "critical_failures": 0,
        "warning_failures": 0,
    }

    for check_name, invalid_df, severity in checks:
        invalid_count = _count(invalid_df)

        results["checks"].append({
            "check_name": check_name,
            "severity": severity,
            "invalid_count": invalid_count,
            "sample_records": _sample_records(invalid_df, sample_limit) if invalid_count > 0 else [],
        })

        if invalid_count > 0:
            if severity == "critical":
                results["critical_failures"] += 1
            else:
                results["warning_failures"] += 1

        write_invalid_records(
            invalid_df=invalid_df,
            bucket_name="brasileirao",
            quality_prefix="quality",
            check_name=check_name,
            run_date=datetime.now().strftime("%Y-%m-%d"),
            dataset_name="silver_matches",
        )

    return results


def raise_if_critical_failures(report: Dict) -> None:
    failing_checks = [
        check for check in report["checks"]
        if check["severity"] == "critical" and check["invalid_count"] > 0
    ]

    if not failing_checks:
        return

    lines = ["silver.matches validation failed:"]
    for check in failing_checks:
        lines.append(f"- {check['check_name']}: {check['invalid_count']} invalid rows")

    raise ValueError("\n".join(lines))

def write_invalid_records(
    invalid_df: DataFrame,
    bucket_name: str,
    quality_prefix: str,
    check_name: str,
    run_date: str,
    dataset_name: str,
) -> None:
    if invalid_df.limit(1).count() == 0:
        return

    path = f"s3a://{bucket_name}/{quality_prefix}/{dataset_name}/check_name={check_name}/run_date={run_date}/"
    invalid_df.write.mode("overwrite").parquet(path)