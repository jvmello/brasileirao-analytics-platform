import pandas as pd
import streamlit as st
from api_client import ApiClientError, get_match
from i18n import language_selector, t
from ui import records_to_dataframe, show_api_error, show_dataframe, show_page_header

st.set_page_config(
    page_title="Match Details | Brasileirão Analytics",
    page_icon="🔎",
    layout="wide",
)

language_selector()

show_page_header(
    f"🔎 {t('match_details')}",
    (
        "Inspect match metadata, goals, cards and team statistics."
        if st.session_state["language"] == "en"
        else "Inspecione metadados da partida, gols, cartões e estatísticas dos times."
    ),
)

with st.sidebar:
    st.header(t("filters"))

    match_id = st.number_input(
        "Match ID" if st.session_state["language"] == "en" else "ID da Partida",
        min_value=1,
        value=8406,
        step=1,
    )

try:
    data = get_match(int(match_id))

    if not data:
        st.info(t("match_not_found"))
        st.stop()

    match = data.get("match", {})
    teams = data.get("teams", {})
    score = data.get("score", {})
    goals = data.get("goals", [])
    cards = data.get("cards", [])
    statistics = data.get("statistics", {})

    st.subheader(f"{t('match_details')} {match_id}")

    if teams and score:
        home_team = teams.get("home", {}).get("name") or teams.get("home_team") or "-"
        away_team = teams.get("away", {}).get("name") or teams.get("away_team") or "-"

        home_score = (
            score.get("home")
            if score.get("home") is not None
            else score.get("home_score")
        )
        away_score = (
            score.get("away")
            if score.get("away") is not None
            else score.get("away_score")
        )

        st.markdown(f"## {home_team} {home_score} x {away_score} {away_team}")

    st.subheader(t("match_metadata"))

    if match:
        st.json(match)
    else:
        st.json(data)

    st.subheader(t("goals"))

    goals_df = records_to_dataframe(goals)
    show_dataframe(goals_df)

    st.subheader(t("cards"))

    cards_df = records_to_dataframe(cards)
    show_dataframe(cards_df)

    st.subheader(t("team_statistics"))

    if isinstance(statistics, dict):
        home_stats = statistics.get("home")
        away_stats = statistics.get("away")

        if home_stats or away_stats:
            stats_rows = []

            if home_stats:
                stats_rows.append({"match_side": "home", **home_stats})

            if away_stats:
                stats_rows.append({"match_side": "away", **away_stats})

            stats_df = pd.DataFrame(stats_rows)
            show_dataframe(stats_df)
        else:
            st.json(statistics)
    else:
        stats_df = records_to_dataframe(statistics)
        show_dataframe(stats_df)

except ApiClientError as exc:
    show_api_error(exc)
