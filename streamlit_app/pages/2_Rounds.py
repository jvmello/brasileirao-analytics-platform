import streamlit as st

from api_client import ApiClientError, get_round_matches
from ui import records_to_dataframe, show_api_error, show_dataframe, show_page_header


st.set_page_config(
    page_title="Rounds | Brasileirão Analytics",
    page_icon="📅",
    layout="wide",
)

show_page_header(
    "📅 Matches by Round",
    "Browse matches from a specific season and round.",
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
        value=1,
        step=1,
    )

try:
    data = get_round_matches(
        season=int(season),
        round_number=int(round_number),
    )

    df = records_to_dataframe(data)

    st.subheader(f"Season {season} - Round {round_number}")

    if df.empty:
        st.info("No matches found for the selected filters.")
        st.stop()

    preferred_columns = [
        "match_id",
        "season",
        "round",
        "match_date",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "stadium_name",
        "stadium_city",
        "stadium_state",
    ]

    existing_columns = [column for column in preferred_columns if column in df.columns]
    remaining_columns = [column for column in df.columns if column not in existing_columns]

    show_dataframe(df[existing_columns + remaining_columns])

except ApiClientError as exc:
    show_api_error(exc)