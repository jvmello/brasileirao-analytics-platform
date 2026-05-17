import pandas as pd
import streamlit as st

from api_client import ApiClientError, get_standings, get_team_dashboard
from ui import (
    get_team_options_from_standings,
    metric_value,
    records_to_dataframe,
    select_team,
    show_api_error,
    show_dataframe,
    show_page_header,
)


st.set_page_config(
    page_title="Team Dashboard | Brasileirão Analytics",
    page_icon="📊",
    layout="wide",
)

show_page_header(
    "📊 Team Dashboard",
    "Season summary, home and away performance, recent matches, scorers and discipline.",
)

with st.sidebar:
    st.header("Filters")

    season = st.number_input(
        "Season",
        min_value=2003,
        max_value=2026,
        value=2024,
        step=1,
    )

    reference_round = st.number_input(
        "Reference round for team selector",
        min_value=1,
        max_value=38,
        value=38,
        step=1,
    )

try:
    standings_data = get_standings(
        season=int(season),
        round_number=int(reference_round),
    )

    team_options = get_team_options_from_standings(standings_data)

    with st.sidebar:
        team_id = select_team(
            label="Team",
            team_options=team_options,
            fallback_key="team_dashboard_team_id",
        )

    dashboard = get_team_dashboard(
        season=int(season),
        team_id=int(team_id),
    )

    if not dashboard:
        st.info("No dashboard data found for the selected team and season.")
        st.stop()

    team = dashboard.get("team", {})
    summary = dashboard.get("summary", {}) or {}
    home_away = dashboard.get("home_away", []) or []
    recent_matches = dashboard.get("recent_matches", []) or []
    top_scorers = dashboard.get("top_scorers", []) or []
    discipline = dashboard.get("discipline", {}) or {}

    st.subheader(f"{team.get('name', 'Team')} - {season}")

    cols = st.columns(5)

    cols[0].metric("Points", metric_value(summary.get("points")))
    cols[1].metric("Matches", metric_value(summary.get("matches_played")))
    cols[2].metric("Wins", metric_value(summary.get("wins")))
    cols[3].metric("Goal Difference", metric_value(summary.get("goal_difference")))
    cols[4].metric("Clean Sheets", metric_value(summary.get("clean_sheets")))

    st.divider()

    st.subheader("Season Summary")

    summary_df = pd.DataFrame([summary])
    show_dataframe(summary_df)

    st.subheader("Home vs Away")

    home_away_df = records_to_dataframe(home_away)
    show_dataframe(home_away_df)

    if not home_away_df.empty and {"match_side", "points"}.issubset(home_away_df.columns):
        chart_df = home_away_df[["match_side", "points"]].set_index("match_side")
        st.bar_chart(chart_df)

    st.subheader("Recent Matches")

    recent_matches_df = records_to_dataframe(recent_matches)
    show_dataframe(recent_matches_df)

    st.subheader("Top Scorers")

    top_scorers_df = records_to_dataframe(top_scorers)
    show_dataframe(top_scorers_df)

    if not top_scorers_df.empty and {"player_name", "goals"}.issubset(top_scorers_df.columns):
        chart_df = (
            top_scorers_df[["player_name", "goals"]]
            .sort_values("goals", ascending=False)
            .set_index("player_name")
        )

        st.bar_chart(chart_df)

    st.subheader("Discipline")

    discipline_df = pd.DataFrame([discipline]) if discipline else pd.DataFrame()
    show_dataframe(discipline_df)

except ApiClientError as exc:
    show_api_error(exc)