from app.db.session import get_connection

def get_standings(season: int, round: int):
    conn = get_connection()
    cur = conn.cursor()

    query = """
    SELECT 
        t.team_name,
        SUM(s.points) as points,
        SUM(s.goals_for) as goals_for,
        SUM(s.goals_against) as goals_against
    FROM analytics.fact_standings s
    JOIN analytics.dim_team t ON s.team_id = t.team_id
    WHERE s.season = %s
      AND s.round <= %s
    GROUP BY t.team_name
    ORDER BY points DESC, (goals_for - goals_against) DESC
    """

    cur.execute(query, (season, round))
    result = cur.fetchall()

    cur.close()
    conn.close()

    return result