from app.db.session import get_connection


def get_standings(season: int, round: int | None = None):
    params = [season]

    round_filter = ""
    if round is not None:
        round_filter = "AND round <= %s"
        params.append(round)

    query = f"""
        SELECT
            team_id,
            team,
            COUNT(*) AS matches,
            SUM(win_flag) AS wins,
            SUM(draw_flag) AS draws,
            SUM(loss_flag) AS losses,
            SUM(goals_scored) AS goals_for,
            SUM(goals_conceded) AS goals_against,
            SUM(goals_scored) - SUM(goals_conceded) AS goal_difference,
            SUM(match_points) AS points
        FROM analytics.fact_team_match_statistics
        WHERE season = %s
        {round_filter}
        GROUP BY team_id, team
        ORDER BY
            points DESC,
            wins DESC,
            goal_difference DESC,
            goals_for DESC,
            team ASC
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