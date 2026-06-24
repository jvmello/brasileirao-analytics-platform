import streamlit as st
from api_client import API_BASE_URL
from i18n import language_selector, t

st.set_page_config(
    page_title="Brasileirão Analytics",
    page_icon="⚽",
    layout="wide",
)

language_selector()

st.title(f"⚽ {t('app_title')}")
st.caption(t("app_description"))

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

st.subheader("API")
st.write(f"API Base URL: `{API_BASE_URL}`")
