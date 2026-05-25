from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from i18n import t, translate_columns


def show_page_header(title: str, description: str | None = None) -> None:
    st.title(title)

    if description:
        st.caption(description)


def show_api_error(error: Exception) -> None:
    st.error(t("api_error"))
    st.exception(error)


def records_to_dataframe(data: Any) -> pd.DataFrame:
    if data is None:
        return pd.DataFrame()

    if isinstance(data, list):
        return pd.DataFrame(data)

    if isinstance(data, dict):
        for key in ["data", "results", "matches", "standings", "items"]:
            value = data.get(key)
            if isinstance(value, list):
                return pd.DataFrame(value)

        return pd.DataFrame([data])

    return pd.DataFrame()


def get_team_options_from_standings(standings: Any) -> dict[str, int]:
    df = records_to_dataframe(standings)

    if df.empty:
        return {}

    if "team_id" not in df.columns or "team_name" not in df.columns:
        return {}

    df = df[["team_id", "team_name"]].drop_duplicates()
    df = df.sort_values("team_name")

    return {
        f"{row.team_name} ({row.team_id})": int(row.team_id)
        for row in df.itertuples(index=False)
    }


def select_team(
    label: str,
    team_options: dict[str, int],
    fallback_key: str,
) -> int:
    if team_options:
        selected = st.selectbox(label, list(team_options.keys()))
        return team_options[selected]

    return int(
        st.number_input(
            label,
            min_value=1,
            step=1,
            key=fallback_key,
        )
    )


def show_dataframe(df: pd.DataFrame, empty_message: str | None = None) -> None:
    if df.empty:
        st.info(empty_message or t("no_data"))
        return

    st.dataframe(
        translate_columns(df),
        use_container_width=True,
        hide_index=True,
    )


def metric_value(value: Any) -> str:
    if value is None:
        return "-"

    return str(value)