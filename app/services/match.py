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
                    m.match_time,
                    m.match_datetime,

                    m.home_team_id,
                    ht.team_name AS home_team,

                    m.away_team_id,
                    at.team_name AS away_team,

                    m.stadium_id,
                    s.stadium_name,
                    s.city AS stadium_city,
                    s.state AS stadium_state,

                    m.home_score,
                    m.away_score,
                    m.home_result,
                    m.away_result,
                    m.total_goals
                FROM analytics.fact_matches m
                JOIN analytics.dim_team ht 
                    ON ht.id = m.home_team_id
                JOIN analytics.dim_team at 
                    ON at.id = m.away_team_id
                LEFT JOIN analytics.dim_stadium s
                    ON s.id = m.stadium_id
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
                    g.minute_base,
                    g.stoppage_minute,
                    g.minute_bucket,
                    g.goal_type,
                    g.team_id,
                    t.team_name,
                    g.player_id,
                    p.player_name
                FROM analytics.fact_goals g
                LEFT JOIN analytics.dim_team t 
                    ON t.id = g.team_id
                LEFT JOIN analytics.dim_player p 
                    ON p.id = g.player_id
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
                    c.minute_base,
                    c.stoppage_minute,
                    c.minute_bucket,
                    c.card_type,
                    c.team_id,
                    t.team_name,
                    c.player_id,
                    p.player_name
                FROM analytics.fact_cards c
                LEFT JOIN analytics.dim_team t 
                    ON t.id = c.team_id
                LEFT JOIN analytics.dim_player p 
                    ON p.id = c.player_id
                WHERE c.match_id = %s
                ORDER BY c.minute
            """,
                (match_id,),
            )
            cards = cur.fetchall()

            cur.execute(
                """
                SELECT
                    s.*,
                    t.team_name,
                    ot.team_name AS opponent_team_name
                FROM analytics.fact_team_match_statistics s
                LEFT JOIN analytics.dim_team t
                    ON t.id = s.team_id
                LEFT JOIN analytics.dim_team ot
                    ON ot.id = s.opponent_team_id
                WHERE s.match_id = %s
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
        "stadium": {
            "id": match["stadium_id"],
            "name": match["stadium_name"],
            "city": match["stadium_city"],
            "state": match["stadium_state"],
        },
    }
