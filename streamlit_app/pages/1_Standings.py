import streamlit as st

from api_client import ApiClientError, get_standings
from i18n import language_selector, t
from ui import records_to_dataframe, show_api_error, show_dataframe


st.set_page_config(
    page_title="Standings | Brasileirão Analytics",
    page_icon="🏆",
    layout="wide",
)

language_selector()

st.title(f"🏆 {t('standings')}")

with st.sidebar:
    st.header("Filters")

    season = st.number_input(
        t("season"),
        min_value=2003,
        max_value=2026,
        value=2024,
        step=1,
    )

    round_number = st.number_input(
        t("round"),
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
        st.info(t("no_data"))
        st.stop()

    cols = st.columns(4)

    leader = df.iloc[0]

    cols[0].metric(t("team"), len(df))
    cols[1].metric("Leader" if st.session_state["language"] == "en" else "Líder", leader.get("team_name", "-"))
    cols[2].metric(t("points"), leader.get("points", "-"))
    cols[3].metric(t("wins"), leader.get("wins", "-"))

    show_dataframe(df)

except ApiClientError as exc:
    st.error(t("api_error"))
    st.exception(exc)