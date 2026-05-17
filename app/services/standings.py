from app.db.session import get_connection


def get_standings(season: int, round: int | None = None):
    params = [season]

    round_filter = ""
    if round is not None:
        round_filter = "AND round <= %s"
        params.append(round)

    query = f"""
        SELECT
            s.team_id,
            t.team_name,
            COUNT(*) AS matches_played,
            SUM(s.win_flag) AS wins,
            SUM(s.draw_flag) AS draws,
            SUM(s.loss_flag) AS losses,
            SUM(s.goals_scored) AS goals_for,
            SUM(s.goals_conceded) AS goals_against,
            SUM(s.goals_scored - s.goals_conceded) AS goal_difference,
            SUM(s.match_points) AS points
        FROM analytics.fact_team_match_statistics s
        JOIN analytics.dim_team t
            ON t.id = s.team_id
        WHERE s.season = %s {round_filter}
        GROUP BY
            s.team_id,
            t.team_name
        ORDER BY
            points DESC,
            wins DESC,
            goal_difference DESC,
            goals_for DESC,
            team_name ASC
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            rows = cur.fetchall()

    standings = []
    for idx, row in enumerate(rows, start=1):
        row["position"] = idx
        standings.append(row)

    return {
        "season": season,
        "round": round,
        "standings": standings,
    }
