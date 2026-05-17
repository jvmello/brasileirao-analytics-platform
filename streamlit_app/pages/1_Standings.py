import pandas as pd
import streamlit as st

from api_client import ApiClientError, get_standings
from ui import records_to_dataframe, show_api_error, show_dataframe, show_page_header


st.set_page_config(
    page_title="Standings | Brasileirão Analytics",
    page_icon="🏆",
    layout="wide",
)

show_page_header(
    "🏆 Standings",
    "League table by season and round.",
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

    round_number = st.number_input(
        "Round",
        min_value=1,
        max_value=38,
        value=38,
        step=1,
    )

try:
    data = get_standings(
        season=int(season),
        round_number=int(round_number),
    )

    df = records_to_dataframe(data)

    if df.empty:
        st.info("No standings found for the selected filters.")
        st.stop()

    st.subheader(f"Season {season} - Round {round_number}")

    metric_cols = st.columns(4)

    leader = df.iloc[0] if not df.empty else None

    metric_cols[0].metric("Teams", len(df))

    if leader is not None:
        metric_cols[1].metric("Leader", leader.get("team_name", "-"))
        metric_cols[2].metric("Points", leader.get("points", "-"))
        metric_cols[3].metric("Wins", leader.get("wins", "-"))

    preferred_columns = [
        "position",
        "team_id",
        "team_name",
        "points",
        "matches_played",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "goal_difference",
    ]

    existing_columns = [column for column in preferred_columns if column in df.columns]
    remaining_columns = [column for column in df.columns if column not in existing_columns]

    show_dataframe(df[existing_columns + remaining_columns])

    if {"team_name", "points"}.issubset(df.columns):
        st.subheader("Points by team")

        chart_df = (
            df[["team_name", "points"]]
            .copy()
            .sort_values("points", ascending=False)
            .set_index("team_name")
        )

        st.bar_chart(chart_df)

except ApiClientError as exc:
    show_api_error(exc)