import streamlit as st

from api_client import API_BASE_URL


st.set_page_config(
    page_title="Brasileirão Analytics",
    page_icon="⚽",
    layout="wide",
)

st.title("⚽ Brasileirão Analytics Platform")

st.markdown(
    """
    Welcome to the Brasileirão Analytics dashboard.

    This Streamlit application consumes the FastAPI backend and displays curated
    football analytics from the PostgreSQL serving layer.
    """
)

st.divider()

st.subheader("Architecture")

st.code(
    """
CSV Files
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Gold Layer
   ↓
PostgreSQL
   ↓
FastAPI
   ↓
Streamlit
""",
    language="text",
)

st.subheader("Current API")

st.write(f"API Base URL: `{API_BASE_URL}`")

st.subheader("Available pages")

st.markdown(
    """
    - **Standings**: league table by season and round
    - **Rounds**: matches from a specific round
    - **Team Dashboard**: team-level summary, recent matches, scorers and discipline
    - **Match Details**: match-level details
    - **Head-to-Head**: historical comparison between two teams
    """
)

st.info("Use the sidebar navigation to open a dashboard page.")