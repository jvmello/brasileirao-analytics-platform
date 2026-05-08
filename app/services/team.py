from app.db.session import get_connection


def get_team_dashboard(season: int, team_id: int):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    team_id,
                    team_name
                FROM analytics.dim_team
                WHERE team_id = %s
            """, (team_id,))
            team = cur.fetchone()

            if not team:
                return None

            cur.execute("""
                SELECT *
                FROM analytics.team_season_summary
                WHERE season = %s
                  AND team_id = %s
            """, (season, team_id))
            summary = cur.fetchone()

            cur.execute("""
                SELECT *
                FROM analytics.team_home_away_summary
                WHERE season = %s
                  AND team_id = %s
                ORDER BY match_side
            """, (season, team_id))
            home_away = cur.fetchall()

            cur.execute("""
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
                JOIN analytics.fact_matches m ON m.match_id = s.match_id
                JOIN analytics.dim_team ht ON ht.team_id = m.home_team_id
                JOIN analytics.dim_team at ON at.team_id = m.away_team_id
                WHERE s.season = %s
                  AND s.team_id = %s
                ORDER BY m.match_date DESC
                LIMIT 10
            """, (season, team_id))
            recent_matches = cur.fetchall()

            cur.execute("""
                SELECT
                    player_id,
                    player,
                    goals,
                    avg_goal_minute
                FROM analytics.top_scorers_by_season
                WHERE season = %s
                  AND team_id = %s
                ORDER BY goals DESC, player ASC
                LIMIT 10
            """, (season, team_id))
            top_scorers = cur.fetchall()

            cur.execute("""
                SELECT *
                FROM analytics.team_discipline_summary
                WHERE season = %s
                  AND team_id = %s
            """, (season, team_id))
            discipline = cur.fetchone()

    return {
        "team": {
            "team_id": team["team_id"],
            "name": team["team_name"],
            "season": season,
        },
        "summary": summary,
        "home_away": home_away,
        "recent_matches": recent_matches,
        "top_scorers": top_scorers,
        "discipline": discipline,
    }