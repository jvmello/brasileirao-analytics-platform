import streamlit as st

from api_client import ApiClientError, get_head_to_head, get_standings
from ui import (
    get_team_options_from_standings,
    records_to_dataframe,
    select_team,
    show_api_error,
    show_dataframe,
    show_page_header,
)


st.set_page_config(
    page_title="Head-to-Head | Brasileirão Analytics",
    page_icon="⚔️",
    layout="wide",
)

show_page_header(
    "⚔️ Head-to-Head",
    "Compare historical matches between two teams.",
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

    use_season_filter = st.checkbox(
        "Filter head-to-head by season",
        value=True,
    )

try:
    standings_data = get_standings(
        season=int(season),
        round_number=int(reference_round),
    )

    team_options = get_team_options_from_standings(standings_data)

    with st.sidebar:
        team1_id = select_team(
            label="Team 1",
            team_options=team_options,
            fallback_key="h2h_team1_id",
        )

        team2_id = select_team(
            label="Team 2",
            team_options=team_options,
            fallback_key="h2h_team2_id",
        )

    if team1_id == team2_id:
        st.warning("Select two different teams.")
        st.stop()

    data = get_head_to_head(
        team1_id=int(team1_id),
        team2_id=int(team2_id),
        season=int(season) if use_season_filter else None,
    )

    if not data:
        st.info("No head-to-head data found.")
        st.stop()

    st.subheader("Head-to-Head Results")

    if isinstance(data, dict):
        summary = data.get("summary")
        matches = data.get("matches") or data.get("data") or data.get("results")

        if summary:
            st.subheader("Summary")
            st.json(summary)

        if matches:
            matches_df = records_to_dataframe(matches)
            show_dataframe(matches_df)
        else:
            df = records_to_dataframe(data)
            show_dataframe(df)
    else:
        df = records_to_dataframe(data)
        show_dataframe(df)

except ApiClientError as exc:
    show_api_error(exc)