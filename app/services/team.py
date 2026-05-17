from app.db.session import get_connection


def get_team_dashboard(season: int, team_id: int):
    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    s.season,
                    s.team_id,
                    t.team_name,
                    COUNT(*) AS matches_played,
                    SUM(s.match_points) AS points,
                    SUM(s.win_flag) AS wins,
                    SUM(s.draw_flag) AS draws,
                    SUM(s.loss_flag) AS losses,
                    SUM(s.goals_scored) AS goals_for,
                    SUM(s.goals_conceded) AS goals_against,
                    SUM(s.goals_scored - s.goals_conceded) AS goal_difference,
                    ROUND(AVG(s.goals_scored)::numeric, 2) AS avg_goals_scored,
                    ROUND(AVG(s.goals_conceded)::numeric, 2) AS avg_goals_conceded,
                    SUM(s.clean_sheet_flag) AS clean_sheets
                FROM analytics.fact_team_match_statistics s
                JOIN analytics.dim_team t
                    ON t.id = s.team_id
                WHERE
                    s.season = %s
                    AND s.team_id = %s
                GROUP BY
                    s.season,
                    s.team_id,
                    t.team_name
                """,
                (season, team_id),
            )

            summary = cur.fetchone()

            if not summary:
                return None

            cur.execute(
                """
                SELECT
                    s.match_side,
                    COUNT(*) AS matches_played,
                    SUM(s.match_points) AS points,
                    SUM(s.win_flag) AS wins,
                    SUM(s.draw_flag) AS draws,
                    SUM(s.loss_flag) AS losses,
                    SUM(s.goals_scored) AS goals_for,
                    SUM(s.goals_conceded) AS goals_against,
                    SUM(s.goals_scored - s.goals_conceded) AS goal_difference
                FROM analytics.fact_team_match_statistics s
                WHERE
                    s.season = %s
                    AND s.team_id = %s
                GROUP BY
                    s.match_side
                ORDER BY
                    s.match_side
                """,
                (season, team_id),
            )

            home_away = cur.fetchall()

            cur.execute(
                """
                SELECT
                    m.match_id,
                    m.round,
                    m.match_date,
                    ht.team_name AS home_team,
                    at.team_name AS away_team,
                    m.home_score,
                    m.away_score,
                    s.match_result,
                    s.match_side
                FROM analytics.fact_team_match_statistics s
                JOIN analytics.fact_matches m
                    ON m.match_id = s.match_id
                JOIN analytics.dim_team ht
                    ON ht.id = m.home_team_id
                JOIN analytics.dim_team at
                    ON at.id = m.away_team_id
                WHERE
                    s.season = %s
                    AND s.team_id = %s
                ORDER BY
                    m.match_date DESC,
                    m.match_id DESC
                LIMIT 10
                """,
                (season, team_id),
            )

            recent_matches = cur.fetchall()

            cur.execute(
                """
                SELECT
                    g.player_id,
                    p.player_name,
                    COUNT(*) AS goals,
                    ROUND(AVG(g.minute)::numeric, 2) AS avg_goal_minute
                FROM analytics.fact_goals g
                JOIN analytics.dim_player p
                    ON p.id = g.player_id
                WHERE
                    g.season = %s
                    AND g.team_id = %s
                GROUP BY
                    g.player_id,
                    p.player_name
                ORDER BY
                    goals DESC,
                    p.player_name ASC
                LIMIT 10
                """,
                (season, team_id),
            )

            top_scorers = cur.fetchall()

            cur.execute(
                """
                SELECT
                    s.season,
                    s.team_id,
                    t.team_name,
                    SUM(s.fouls) AS fouls,
                    SUM(s.yellow_cards) AS yellow_cards,
                    SUM(s.red_cards) AS red_cards,
                    ROUND(AVG(s.fouls)::numeric, 2) AS avg_fouls_per_match,
                    ROUND(AVG(s.yellow_cards)::numeric, 2) AS avg_yellow_cards_per_match,
                    ROUND(AVG(s.red_cards)::numeric, 2) AS avg_red_cards_per_match
                FROM analytics.fact_team_match_statistics s
                JOIN analytics.dim_team t
                    ON t.id = s.team_id
                WHERE
                    s.season = %s
                    AND s.team_id = %s
                GROUP BY
                    s.season,
                    s.team_id,
                    t.team_name
                """,
                (season, team_id),
            )

            discipline = cur.fetchone()

    finally:
        conn.close()

    return {
        "team": {
            "team_id": summary["team_id"],
            "name": summary["team_name"],
            "season": season,
        },
        "summary": summary,
        "home_away": home_away,
        "recent_matches": recent_matches,
        "top_scorers": top_scorers,
        "discipline": discipline,
    }
