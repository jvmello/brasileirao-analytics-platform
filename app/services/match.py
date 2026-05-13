from app.db.session import get_connection


def get_match(match_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    m.match_id,
                    m.season,
                    m.round,
                    m.match_date,
                    m.stadium,
                    m.home_team_id,
                    ht.team_name AS home_team,
                    m.away_team_id,
                    at.team_name AS away_team,
                    m.home_score,
                    m.away_score
                FROM analytics.fact_matches m
                JOIN analytics.dim_team ht ON ht.team_id = m.home_team_id
                JOIN analytics.dim_team at ON at.team_id = m.away_team_id
                WHERE m.match_id = %s
            """,
                (match_id,),
            )
            match = cur.fetchone()

            if not match:
                return None

            cur.execute(
                """
                SELECT
                    g.minute,
                    g.minute_raw,
                    g.goal_type,
                    g.team_id,
                    t.team_name,
                    p.player_name
                FROM analytics.fact_goals g
                LEFT JOIN analytics.dim_team t ON t.team_id = g.team_id
                LEFT JOIN analytics.dim_player p ON p.player_id = g.player_id
                WHERE g.match_id = %s
                ORDER BY g.minute
            """,
                (match_id,),
            )
            goals = cur.fetchall()

            cur.execute(
                """
                SELECT
                    c.minute,
                    c.minute_raw,
                    c.card_type,
                    c.team_id,
                    t.team_name,
                    p.player_name
                FROM analytics.fact_cards c
                LEFT JOIN analytics.dim_team t ON t.team_id = c.team_id
                LEFT JOIN analytics.dim_player p ON p.player_id = c.player_id
                WHERE c.match_id = %s
                ORDER BY c.minute
            """,
                (match_id,),
            )
            cards = cur.fetchall()

            cur.execute(
                """
                SELECT *
                FROM analytics.fact_team_match_statistics
                WHERE match_id = %s
            """,
                (match_id,),
            )
            stats_rows = cur.fetchall()

    home_stats = next(
        (s for s in stats_rows if s["team_id"] == match["home_team_id"]),
        None,
    )

    away_stats = next(
        (s for s in stats_rows if s["team_id"] == match["away_team_id"]),
        None,
    )

    return {
        "match": {
            "match_id": match["match_id"],
            "season": match["season"],
            "round": match["round"],
            "match_date": str(match["match_date"]),
            "stadium": match["stadium"],
        },
        "teams": {
            "home": {
                "team_id": match["home_team_id"],
                "name": match["home_team"],
            },
            "away": {
                "team_id": match["away_team_id"],
                "name": match["away_team"],
            },
        },
        "score": {
            "home": match["home_score"],
            "away": match["away_score"],
        },
        "goals": goals,
        "cards": cards,
        "statistics": {
            "home": home_stats,
            "away": away_stats,
        },
    }
