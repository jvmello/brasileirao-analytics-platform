import streamlit as st
from api_client import ApiClientError, get_head_to_head, get_standings
from i18n import language_selector, t
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

language_selector()

show_page_header(
    f"⚔️ {t('head_to_head')}",
    (
        "Compare historical matches between two teams."
        if st.session_state["language"] == "en"
        else "Compare o histórico de partidas entre dois times."
    ),
)

with st.sidebar:
    st.header(t("filters"))

    season = st.number_input(
        t("season"),
        min_value=2003,
        max_value=2026,
        value=2024,
        step=1,
    )

    reference_round = st.number_input(
        t("reference_round"),
        min_value=1,
        max_value=38,
        value=38,
        step=1,
    )

    use_season_filter = st.checkbox(
        t("filter_by_season"),
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
            label=t("team_1"),
            team_options=team_options,
            fallback_key="h2h_team1_id",
        )

        team2_id = select_team(
            label=t("team_2"),
            team_options=team_options,
            fallback_key="h2h_team2_id",
        )

    if team1_id == team2_id:
        st.warning(t("different_teams_warning"))
        st.stop()

    data = get_head_to_head(
        team1_id=int(team1_id),
        team2_id=int(team2_id),
        season=int(season) if use_season_filter else None,
    )

    if not data:
        st.info(t("no_h2h"))
        st.stop()

    st.subheader(t("head_to_head_results"))

    if isinstance(data, dict):
        summary = data.get("summary")
        matches = data.get("matches") or data.get("data") or data.get("results")

        if summary:
            st.subheader(t("summary"))
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
