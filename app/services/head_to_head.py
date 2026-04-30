from app.db.session import get_connection

def get_head_to_head(team1_id: int, team2_id: int):
    conn = get_connection()
    cur = conn.cursor()

    query = """
    SELECT 
        m.match_id,
        m.match_date,
        ht.team_name as home_team,
        at.team_name as away_team,
        m.home_score,
        m.away_score
    FROM analytics.fact_matches m
    JOIN analytics.dim_team ht ON m.home_team_id = ht.team_id
    JOIN analytics.dim_team at ON m.away_team_id = at.team_id
    WHERE 
        (m.home_team_id = %s AND m.away_team_id = %s)
        OR
        (m.home_team_id = %s AND m.away_team_id = %s)
    ORDER BY m.match_date DESC
    """

    cur.execute(query, (team1_id, team2_id, team2_id, team1_id))
    matches = cur.fetchall()

    # resumo
    summary = {
        "team1_wins": 0,
        "team2_wins": 0,
        "draws": 0
    }

    for m in matches:
        if m["home_score"] == m["away_score"]:
            summary["draws"] += 1
        elif (
            (m["home_team_id"] == team1_id and m["home_score"] > m["away_score"]) or
            (m["away_team_id"] == team1_id and m["away_score"] > m["home_score"])
        ):
            summary["team1_wins"] += 1
        else:
            summary["team2_wins"] += 1

    cur.close()
    conn.close()

    return {
        "summary": summary,
        "matches": matches
    }