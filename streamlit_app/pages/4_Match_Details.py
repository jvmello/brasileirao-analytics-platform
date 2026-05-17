import pandas as pd
import streamlit as st

from api_client import ApiClientError, get_match
from ui import records_to_dataframe, show_api_error, show_dataframe, show_page_header


st.set_page_config(
    page_title="Match Details | Brasileirão Analytics",
    page_icon="🔎",
    layout="wide",
)

show_page_header(
    "🔎 Match Details",
    "Inspect match metadata, goals, cards and team statistics.",
)

with st.sidebar:
    st.header("Filters")

    match_id = st.number_input(
        "Match ID",
        min_value=1,
        value=8406,
        step=1,
    )

try:
    data = get_match(int(match_id))

    if not data:
        st.info("Match not found.")
        st.stop()

    match = data.get("match", {})
    teams = data.get("teams", {})
    score = data.get("score", {})
    goals = data.get("goals", [])
    cards = data.get("cards", [])
    statistics = data.get("statistics", {})

    st.subheader(f"Match {match_id}")

    if teams and score:
        home_team = teams.get("home", {}).get("name") or teams.get("home_team") or "-"
        away_team = teams.get("away", {}).get("name") or teams.get("away_team") or "-"

        home_score = score.get("home") or score.get("home_score")
        away_score = score.get("away") or score.get("away_score")

        st.markdown(f"## {home_team} {home_score} x {away_score} {away_team}")

    st.subheader("Match Metadata")

    if match:
        st.json(match)
    else:
        st.json(data)

    st.subheader("Goals")

    goals_df = records_to_dataframe(goals)
    show_dataframe(goals_df)

    st.subheader("Cards")

    cards_df = records_to_dataframe(cards)
    show_dataframe(cards_df)

    st.subheader("Team Statistics")

    if isinstance(statistics, dict):
        home_stats = statistics.get("home")
        away_stats = statistics.get("away")

        if home_stats or away_stats:
            stats_df = pd.DataFrame(
                [
                    {"side": "home", **home_stats} if home_stats else {"side": "home"},
                    {"side": "away", **away_stats} if away_stats else {"side": "away"},
                ]
            )

            show_dataframe(stats_df)
        else:
            st.json(statistics)
    else:
        stats_df = records_to_dataframe(statistics)
        show_dataframe(stats_df)

except ApiClientError as exc:
    show_api_error(exc)