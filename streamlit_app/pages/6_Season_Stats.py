import pandas as pd
import streamlit as st

from api_client import ApiClientError, get_team_season_stats
from i18n import language_selector, t, translate_columns
from ui import records_to_dataframe, show_api_error, show_dataframe, show_page_header


st.set_page_config(
    page_title="Season Stats | Brasileirão Analytics",
    page_icon="📈",
    layout="wide",
)

language_selector()

language = st.session_state.get("language", "en")

title = "📈 Season Stats" if language == "en" else "📈 Estatísticas da Temporada"
description = (
    "Season-level team statistics with expandable metric tables."
    if language == "en"
    else "Resumo estatístico da temporada com tabelas detalhadas por métrica."
)

show_page_header(title, description)

@st.dialog("Metric details", width="large")
def show_metric_details_dialog(
    df: pd.DataFrame,
    metric_key: str,
    metric_title: str,
) -> None:
    st.subheader(metric_title)

    metric_table = prepare_metric_table(df, metric_key)

    show_dataframe(metric_table)

    st.divider()

    st.subheader("Chart" if language == "en" else "Gráfico")

    chart_df = (
        metric_table[["team_name", metric_key]]
        .set_index("team_name")
        .sort_values(metric_key, ascending=True)
    )

    st.bar_chart(chart_df)

    if st.button("Close" if language == "en" else "Fechar"):
        st.rerun()


METRICS = {
    "goals_for": {
        "label_en": "Most Goals",
        "label_pt": "Mais Gols",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "shots": {
        "label_en": "Most Shots",
        "label_pt": "Mais Chutes",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "shots_on_target": {
        "label_en": "Most Shots on Target",
        "label_pt": "Mais Chutes no Alvo",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "shot_accuracy": {
        "label_en": "Best Shot Accuracy",
        "label_pt": "Melhor Precisão nos Chutes",
        "higher_is_better": True,
        "value_suffix": "%",
    },
    "scoring_efficiency": {
        "label_en": "Best Scoring Efficiency",
        "label_pt": "Melhor Eficiência Ofensiva",
        "higher_is_better": True,
        "value_suffix": "%",
    },
    "avg_possession": {
        "label_en": "Highest Possession",
        "label_pt": "Mais Posse de Bola",
        "higher_is_better": True,
        "value_suffix": "%",
    },
    "passes": {
        "label_en": "Most Passes",
        "label_pt": "Mais Passes",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "avg_pass_accuracy": {
        "label_en": "Best Pass Accuracy",
        "label_pt": "Melhor Precisão nos Passes",
        "higher_is_better": True,
        "value_suffix": "%",
    },
    "fouls": {
        "label_en": "Most Fouls",
        "label_pt": "Mais Faltas",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "yellow_cards": {
        "label_en": "Most Yellow Cards",
        "label_pt": "Mais Cartões Amarelos",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "red_cards": {
        "label_en": "Most Red Cards",
        "label_pt": "Mais Cartões Vermelhos",
        "higher_is_better": True,
        "value_suffix": "",
    },
    "clean_sheets": {
        "label_en": "Most Clean Sheets",
        "label_pt": "Mais Jogos sem Sofrer Gol",
        "higher_is_better": True,
        "value_suffix": "",
    },
}


def metric_label(metric_key: str) -> str:
    metric = METRICS[metric_key]

    if language == "pt":
        return metric["label_pt"]

    return metric["label_en"]


def format_metric_value(value, suffix: str = "") -> str:
    if value is None:
        return "-"

    if isinstance(value, float):
        return f"{value:.2f}{suffix}"

    return f"{value}{suffix}"


def get_metric_leader(df: pd.DataFrame, metric_key: str):
    metric = METRICS[metric_key]

    metric_df = df[["team_name", metric_key]].dropna()

    if metric_df.empty:
        return None

    return metric_df.sort_values(
        metric_key,
        ascending=not metric["higher_is_better"],
    ).iloc[0]


def unique_columns(columns: list[str]) -> list[str]:
    seen = set()
    result = []

    for column in columns:
        if column not in seen:
            result.append(column)
            seen.add(column)

    return result


def prepare_metric_table(df: pd.DataFrame, metric_key: str) -> pd.DataFrame:
    metric = METRICS[metric_key]

    base_columns = [
        "team_name",
        metric_key,
        "matches_played",
        "points",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "goal_difference",
    ]

    columns = unique_columns(base_columns)

    existing_columns = [
        column for column in columns
        if column in df.columns
    ]

    table = df[existing_columns].copy()

    table = table.sort_values(
        metric_key,
        ascending=not metric["higher_is_better"],
    )

    table.insert(0, "position", range(1, len(table) + 1))

    return table


with st.sidebar:
    st.header(t("filters"))

    season = st.number_input(
        t("season"),
        min_value=2003,
        max_value=2026,
        value=2024,
        step=1,
    )

    use_round_filter = st.checkbox(
        "Use round filter" if language == "en" else "Usar filtro de rodada",
        value=True,
    )

    round_number = None

    if use_round_filter:
        round_number = st.number_input(
            t("round"),
            min_value=1,
            max_value=38,
            value=38,
            step=1,
        )


if "selected_season_metric" not in st.session_state:
    st.session_state["selected_season_metric"] = "goals_for"


try:
    data = get_team_season_stats(
        season=int(season),
        round_number=int(round_number) if round_number is not None else None,
    )

    df = records_to_dataframe(data)

    if df.empty:
        st.info(t("no_data"))
        st.stop()

    subtitle = (
        f"{t('season')} {season}"
        if round_number is None
        else f"{t('season')} {season} - {t('round')} {round_number}"
    )

    st.subheader(subtitle)

    st.divider()

    st.subheader("Overview" if language == "en" else "Visão Geral")

    metric_keys = list(METRICS.keys())

    for row_start in range(0, len(metric_keys), 4):
        cols = st.columns(4)

        for col, metric_key in zip(cols, metric_keys[row_start:row_start + 4]):
            leader = get_metric_leader(df, metric_key)
            metric = METRICS[metric_key]

            with col:
                st.markdown(f"#### {metric_label(metric_key)}")

                if leader is None:
                    st.metric("-", "-")
                else:
                    st.metric(
                        leader["team_name"],
                        format_metric_value(
                            leader[metric_key],
                            metric["value_suffix"],
                        ),
                    )

                if st.button(
                    "View table" if language == "en" else "Ver tabela",
                    key=f"btn_{metric_key}",
                    use_container_width=True,
                ):
                    show_metric_details_dialog(
                        df=df,
                        metric_key=metric_key,
                        metric_title=metric_label(metric_key),
                    )

except ApiClientError as exc:
    show_api_error(exc)