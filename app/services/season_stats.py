from app.db.session import get_connection


def get_team_season_stats(season: int, round: int | None = None):
    query = """
        SELECT
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

            SUM(s.shots) AS shots,
            SUM(s.shots_on_target) AS shots_on_target,

            ROUND(
                CASE
                    WHEN SUM(s.shots) > 0
                    THEN (SUM(s.shots_on_target)::numeric / SUM(s.shots)) * 100
                    ELSE NULL
                END,
                2
            ) AS shot_accuracy,

            ROUND(
                CASE
                    WHEN SUM(s.shots) > 0
                    THEN (SUM(s.goals_scored)::numeric / SUM(s.shots)) * 100
                    ELSE NULL
                END,
                2
            ) AS scoring_efficiency,

            ROUND(AVG(s.possession)::numeric, 2) AS avg_possession,
            SUM(s.passes) AS passes,
            ROUND(AVG(s.pass_accuracy)::numeric, 2) AS avg_pass_accuracy,

            SUM(s.fouls) AS fouls,
            SUM(s.yellow_cards) AS yellow_cards,
            SUM(s.red_cards) AS red_cards,
            SUM(s.offsides) AS offsides,
            SUM(s.corners) AS corners,

            SUM(s.clean_sheet_flag) AS clean_sheets
        FROM analytics.fact_team_match_statistics s
        JOIN analytics.dim_team t
            ON t.id = s.team_id
        WHERE
            s.season = %s
    """

    params = [season]

    if round is not None:
        query += """
            AND s.round <= %s
        """
        params.append(round)

    query += """
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

    conn = get_connection()

    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    finally:
        conn.close()