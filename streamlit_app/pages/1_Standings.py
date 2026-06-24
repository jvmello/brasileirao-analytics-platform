import streamlit as st
from api_client import ApiClientError, get_standings
from i18n import language_selector, t
from ui import (
    prepare_standings_table,
    records_to_dataframe,
    show_api_error,
    show_page_header,
)

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

    st.subheader(f"{t('season')} {season} - {t('round')} {round_number}")

    metric_cols = st.columns(4)

    leader = df.iloc[0]

    metric_cols[0].metric(t("team"), len(df))
    metric_cols[1].metric(t("leader"), leader.get("team_name", "-"))
    metric_cols[2].metric(t("points"), leader.get("points", "-"))
    metric_cols[3].metric(t("wins"), leader.get("wins", "-"))

    display_df = prepare_standings_table(df)

    # styled_df = style_standings_table(display_df)

    # st.dataframe(
    #     styled_df,
    #     use_container_width=True,
    #     hide_index=True,
    # )

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
    )

    if {"team_name", "points"}.issubset(df.columns):
        st.subheader(t("points_by_team"))

        chart_df = (
            df[["team_name", "points"]]
            .copy()
            .sort_values("points", ascending=False)
            .set_index("team_name")
        )

        st.bar_chart(chart_df)

except ApiClientError as exc:
    show_api_error(exc)

except ApiClientError as exc:
    st.error(t("api_error"))
    st.exception(exc)
