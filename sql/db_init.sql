-- SCHEMA
CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================
-- FACT MATCHES
-- =========================
CREATE TABLE IF NOT EXISTS analytics.fact_matches (
    match_id VARCHAR PRIMARY KEY,
    season INT,
    round INT,
    date TIMESTAMP,
    home_team VARCHAR,
    away_team VARCHAR,
    home_score INT,
    away_score INT
);

-- =========================
-- FACT GOALS
-- =========================
CREATE TABLE IF NOT EXISTS analytics.fact_goals (
    goal_id VARCHAR PRIMARY KEY,
    match_id VARCHAR REFERENCES analytics.fact_matches(match_id),
    team VARCHAR,
    player VARCHAR,
    minute INT,
    minute_extra INT,
    minute_exact INT
);

-- =========================
-- FACT CARDS
-- =========================
CREATE TABLE IF NOT EXISTS analytics.fact_cards (
    card_id VARCHAR PRIMARY KEY,
    match_id VARCHAR REFERENCES analytics.fact_matches(match_id),
    team VARCHAR,
    player VARCHAR,
    card_type VARCHAR,
    minute INT,
    minute_extra INT,
    minute_exact INT
);

-- =========================
-- MARTS
-- =========================
CREATE TABLE IF NOT EXISTS analytics.team_season_summary (
    season INT,
    team VARCHAR,
    matches INT,
    wins INT,
    draws INT,
    losses INT,
    goals_for INT,
    goals_against INT,
    goal_diff INT,
    points INT,
    PRIMARY KEY (season, team)
);

CREATE TABLE IF NOT EXISTS analytics.team_home_away_summary (
    season INT,
    team VARCHAR,
    home_matches INT,
    home_wins INT,
    away_matches INT,
    away_wins INT,
    PRIMARY KEY (season, team)
);

CREATE TABLE IF NOT EXISTS analytics.top_scorers_by_season (
    season INT,
    player VARCHAR,
    team VARCHAR,
    goals INT,
    PRIMARY KEY (season, player, team)
);

CREATE TABLE IF NOT EXISTS analytics.team_discipline_summary (
    season INT,
    team VARCHAR,
    yellow_cards INT,
    red_cards INT,
    fouls INT,
    PRIMARY KEY (season, team)
);

-- =========================
-- INDEXES
-- =========================
CREATE INDEX IF NOT EXISTS idx_matches_season 
ON analytics.fact_matches(season);

CREATE INDEX IF NOT EXISTS idx_goals_match 
ON analytics.fact_goals(match_id);

CREATE INDEX IF NOT EXISTS idx_cards_match 
ON analytics.fact_cards(match_id);