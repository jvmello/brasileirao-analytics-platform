from app.db.session import get_connection


def get_round_matches(season: int, round: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    m.match_id,
                    m.season,
                    m.round,
                    m.match_date,
                    m.stadium,
                    ht.team_name AS home_team,
                    at.team_name AS away_team,
                    m.home_score,
                    m.away_score
                FROM analytics.fact_matches m
                JOIN analytics.dim_team ht ON ht.team_id = m.home_team_id
                JOIN analytics.dim_team at ON at.team_id = m.away_team_id
                WHERE m.season = %s
                  AND m.round = %s
                ORDER BY m.match_date, m.match_id
            """, (season, round))
            matches = cur.fetchall()

    return {
        "season": season,
        "round": round,
        "matches": matches,
    }