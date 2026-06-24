from __future__ import annotations

import os
from typing import Any

import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")

ENDPOINTS = {
    "standings": "/api/v1/standings",
    "rounds": "/api/v1/rounds",
    "matches": "/api/v1/matches",
    "head_to_head": "/api/v1/head-to-head",
    "team_dashboard": "/api/v1/seasons/{season}/teams/{team_id}/dashboard",
    "team_season_stats": "/api/v1/seasons/{season}/stats/teams",
}


class ApiClientError(Exception):
    pass


def build_url(path: str) -> str:
    return f"{API_BASE_URL}/{path.lstrip('/')}"


@st.cache_data(ttl=300, show_spinner=False)
def api_get(path: str, params: dict[str, Any] | None = None) -> Any:
    url = build_url(path)

    try:
        response = requests.get(url, params=params, timeout=30)

        if response.status_code == 404:
            return None

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as exc:
        raise ApiClientError(f"API request failed: {exc}") from exc


def get_standings(season: int, round_number: int | None = None) -> Any:
    params: dict[str, Any] = {"season": season}

    if round_number is not None:
        params["round"] = round_number

    return api_get(ENDPOINTS["standings"], params=params)


def get_round_matches(season: int, round_number: int) -> Any:
    return api_get(
        ENDPOINTS["rounds"],
        params={
            "season": season,
            "round": round_number,
        },
    )


def get_match(match_id: int) -> Any:
    return api_get(f"{ENDPOINTS['matches']}/{match_id}")


def get_head_to_head(
    team1_id: int,
    team2_id: int,
    season: int | None = None,
) -> Any:
    params: dict[str, Any] = {
        "team1_id": team1_id,
        "team2_id": team2_id,
    }

    if season is not None:
        params["season"] = season

    return api_get(ENDPOINTS["head_to_head"], params=params)


def get_team_dashboard(season: int, team_id: int) -> Any:
    path = ENDPOINTS["team_dashboard"].format(
        season=season,
        team_id=team_id,
    )

    return api_get(path)

def get_team_season_stats(season: int, round_number: int | None = None):
    path = ENDPOINTS["team_season_stats"].format(season=season)

    params = {}

    if round_number is not None:
        params["round"] = round_number

    return api_get(path, params=params)