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


def prepare_standings_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    standings_df = df.copy()

    if "position" not in standings_df.columns:
        standings_df.insert(0, "position", range(1, len(standings_df) + 1))

    if (
        "points" in standings_df.columns
        and "matches_played" in standings_df.columns
        and "performance" not in standings_df.columns
    ):
        standings_df["performance"] = (
            standings_df["points"] / (standings_df["matches_played"] * 3) * 100
        ).round(1)

    column_order = [
        "position",
        "team_name",
        "points",
        "matches_played",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "goal_difference",
        "performance",
    ]

    existing_columns = [
        column for column in column_order if column in standings_df.columns
    ]

    standings_df = standings_df[existing_columns]

    language = st.session_state.get("language", "en")

    if language == "pt":
        standings_df = standings_df.rename(
            columns={
                "position": "#",
                "team_name": "Clube",
                "points": "Pontos",
                "matches_played": "Jogos",
                "wins": "Vitórias",
                "draws": "Empates",
                "losses": "Derrotas",
                "goals_for": "Gols Marcados",
                "goals_against": "Gols Sofridos",
                "goal_difference": "Saldo de Gols",
                "performance": "Aproveitamento (%)",
            }
        )
    else:
        standings_df = standings_df.rename(
            columns={
                "position": "#",
                "team_name": "Club",
                "points": "Points",
                "matches_played": "Matches",
                "wins": "Wins",
                "draws": "Draws",
                "losses": "Losses",
                "goals_for": "Goals For",
                "goals_against": "Goals Against",
                "goal_difference": "Goal Difference",
                "performance": "Performance (%)",
            }
        )

    return standings_df


# TODO Customize to each year
def style_standings_table(df: pd.DataFrame):
    def highlight_row(row):
        position = row.get("#")

        if position is None:
            return [""] * len(row)

        if position <= 4:
            return ["background-color: #e8f5e9"] * len(row)

        if position <= 6:
            return ["background-color: #e3f2fd"] * len(row)

        if position >= 17:
            return ["background-color: #ffebee"] * len(row)

        return [""] * len(row)

    return df.style.apply(highlight_row, axis=1)
