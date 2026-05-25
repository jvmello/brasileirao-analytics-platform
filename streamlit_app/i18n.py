from __future__ import annotations

import streamlit as st


TRANSLATIONS = {
    "en": {
        "language": "Language",
        "filters": "Filters",
        "season": "Season",
        "round": "Round",
        "team": "Team",
        "team_1": "Team 1",
        "team_2": "Team 2",
        "reference_round": "Reference round for team selector",
        "filter_by_season": "Filter by season",
        "standings": "Standings",
        "rounds": "Matches by Round",
        "team_dashboard": "Team Dashboard",
        "match_details": "Match Details",
        "head_to_head": "Head-to-Head",
        "points": "Points",
        "matches": "Matches",
        "wins": "Wins",
        "draws": "Draws",
        "losses": "Losses",
        "leader": "Leader",
        "goals_for": "Goals For",
        "goals_against": "Goals Against",
        "goal_difference": "Goal Difference",
        "clean_sheets": "Clean Sheets",
        "season_summary": "Season Summary",
        "home_away": "Home vs Away",
        "recent_matches": "Recent Matches",
        "top_scorers": "Top Scorers",
        "discipline": "Discipline",
        "match_metadata": "Match Metadata",
        "goals": "Goals",
        "cards": "Cards",
        "team_statistics": "Team Statistics",
        "head_to_head_results": "Head-to-Head Results",
        "summary": "Summary",
        "points_by_team": "Points by team",
        "no_data": "No data found for the selected filters.",
        "no_matches": "No matches found for the selected filters.",
        "no_dashboard": "No dashboard data found for the selected team and season.",
        "match_not_found": "Match not found.",
        "no_h2h": "No head-to-head data found.",
        "different_teams_warning": "Select two different teams.",
        "api_error": "Could not load data from the API.",
    },
    "pt": {
        "language": "Idioma",
        "filters": "Filtros",
        "season": "Temporada",
        "round": "Rodada",
        "team": "Time",
        "team_1": "Time 1",
        "team_2": "Time 2",
        "reference_round": "Rodada de referência para selecionar times",
        "filter_by_season": "Filtrar por temporada",
        "standings": "Classificação",
        "rounds": "Jogos por Rodada",
        "team_dashboard": "Dashboard do Clube",
        "match_details": "Detalhes da Partida",
        "head_to_head": "Confronto Direto",
        "points": "Pontos",
        "matches": "Jogos",
        "wins": "Vitórias",
        "draws": "Empates",
        "losses": "Derrotas",
        "leader": "Líder",
        "goals_for": "Gols Pró",
        "goals_against": "Gols Contra",
        "goal_difference": "Saldo de Gols",
        "clean_sheets": "Jogos sem Sofrer Gol",
        "season_summary": "Resumo da Temporada",
        "home_away": "Casa vs Fora",
        "recent_matches": "Partidas Recentes",
        "top_scorers": "Artilheiros",
        "discipline": "Disciplina",
        "match_metadata": "Metadados da Partida",
        "goals": "Gols",
        "cards": "Cartões",
        "team_statistics": "Estatísticas dos Times",
        "head_to_head_results": "Resultados do Confronto Direto",
        "summary": "Resumo",
        "points_by_team": "Pontos por time",
        "no_data": "Nenhum dado encontrado para os filtros selecionados.",
        "no_matches": "Nenhuma partida encontrada para os filtros selecionados.",
        "no_dashboard": "Nenhum dado de dashboard encontrado para o time e temporada selecionados.",
        "match_not_found": "Partida não encontrada.",
        "no_h2h": "Nenhum dado de confronto direto encontrado.",
        "different_teams_warning": "Selecione dois times diferentes.",
        "api_error": "Não foi possível carregar os dados da API.",
    },
}


COLUMN_TRANSLATIONS = {
    "en": {
        "position": "Position",
        "team_id": "Team ID",
        "team_name": "Team",
        "points": "Points",
        "matches_played": "Matches",
        "wins": "Wins",
        "draws": "Draws",
        "losses": "Losses",
        "goals_for": "Goals For",
        "goals_against": "Goals Against",
        "goal_difference": "Goal Difference",
        "match_id": "Match ID",
        "season": "Season",
        "round": "Round",
        "match_date": "Date",
        "home_team": "Home Team",
        "away_team": "Away Team",
        "home_score": "Home Score",
        "away_score": "Away Score",
        "stadium_name": "Stadium",
        "stadium_city": "City",
        "stadium_state": "State",
        "match_side": "Side",
        "match_result": "Result",
        "player_name": "Player",
        "goals": "Goals",
        "avg_goal_minute": "Avg Goal Minute",
        "yellow_cards": "Yellow Cards",
        "red_cards": "Red Cards",
        "fouls": "Fouls",
    },
    "pt": {
        "position": "Posição",
        "team_id": "ID do Time",
        "team_name": "Time",
        "points": "Pontos",
        "matches_played": "Jogos",
        "wins": "Vitórias",
        "draws": "Empates",
        "losses": "Derrotas",
        "goals_for": "Gols Pró",
        "goals_against": "Gols Contra",
        "goal_difference": "Saldo de Gols",
        "match_id": "ID da Partida",
        "season": "Temporada",
        "round": "Rodada",
        "match_date": "Data",
        "home_team": "Mandante",
        "away_team": "Visitante",
        "home_score": "Placar Mandante",
        "away_score": "Placar Visitante",
        "stadium_name": "Estádio",
        "stadium_city": "Cidade",
        "stadium_state": "Estado",
        "match_side": "Mando",
        "match_result": "Resultado",
        "player_name": "Jogador",
        "goals": "Gols",
        "avg_goal_minute": "Minuto Médio do Gol",
        "yellow_cards": "Cartões Amarelos",
        "red_cards": "Cartões Vermelhos",
        "fouls": "Faltas",
    },
}


LANGUAGE_OPTIONS = {
    "English": "en",
    "Português": "pt",
}


def init_language(default: str = "en") -> None:
    if "language" not in st.session_state:
        st.session_state["language"] = default


def language_selector() -> str:
    init_language()

    current_language = st.session_state["language"]

    current_label = next(
        label
        for label, code in LANGUAGE_OPTIONS.items()
        if code == current_language
    )

    selected_label = st.sidebar.selectbox(
        TRANSLATIONS[current_language]["language"],
        options=list(LANGUAGE_OPTIONS.keys()),
        index=list(LANGUAGE_OPTIONS.keys()).index(current_label),
    )

    selected_language = LANGUAGE_OPTIONS[selected_label]
    st.session_state["language"] = selected_language

    return selected_language


def t(key: str) -> str:
    language = st.session_state.get("language", "en")

    return (
        TRANSLATIONS
        .get(language, TRANSLATIONS["en"])
        .get(key, TRANSLATIONS["en"].get(key, key))
    )


def translate_columns(df):
    language = st.session_state.get("language", "en")
    mapping = COLUMN_TRANSLATIONS.get(language, {})

    return df.rename(columns=mapping)