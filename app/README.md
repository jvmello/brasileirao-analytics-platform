# Brasileirão Analytics Platform API

FastAPI backend for the Brasileirão Analytics Platform.

This API exposes analytical endpoints over the PostgreSQL serving layer populated by the Gold data pipeline.

The API is responsible for serving:

- Match details
- Matches by round
- Standings
- Head-to-head statistics
- Team dashboards

---

## API Architecture

```text
PostgreSQL -> FastAPI -> Streamlit
```

The API does not process raw files directly. It reads curated analytical tables from PostgreSQL.

Database schema:

```text
analytics
```

Main tables used by the API:

```text
analytics.dim_team
analytics.dim_player
analytics.dim_stadium
analytics.fact_matches
analytics.fact_goals
analytics.fact_cards
analytics.fact_team_match_statistics
```

---

## Application Structure

```text
app/
├── main.py
├── db/
│   ├── __init__.py
│   └── session.py
├── routers/
│   ├── __init__.py
│   ├── matches.py
│   ├── rounds.py
│   ├── standings.py
│   ├── head_to_head.py
│   └── teams.py
└── services/
    ├── __init__.py
    ├── match_service.py
    ├── round_service.py
    ├── standings_service.py
    ├── head_to_head.py
    └── team_service.py
```

---

## Running the API

From the project root:

```bash
python -m uvicorn app.main:app --reload
```

Swagger documentation:

```text
http://localhost:8000/docs
```

OpenAPI JSON:

```text
http://localhost:8000/openapi.json
```

Do not run the application with:

```bash
python app/main.py
```

The API should be executed as a module from the project root.

---

## Database Connection

The API connects to PostgreSQL using `psycopg2`.

Example environment variables:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=brasileirao
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

The connection is configured in:

```text
app/db/session.py
```

The cursor should use `RealDictCursor` so service functions can return dictionary-like rows.

Example connection setup:

```python
import os

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def get_connection():
    return psycopg2.connect(
        host=get_env("POSTGRES_HOST", "localhost"),
        port=get_env("POSTGRES_PORT", "5432"),
        dbname=get_env("POSTGRES_DB", "brasileirao"),
        user=get_env("POSTGRES_USER", "postgres"),
        password=get_env("POSTGRES_PASSWORD", "postgres"),
        cursor_factory=RealDictCursor,
    )
```

---

## Endpoints

### Health Check

```http
GET /
```

Returns a basic API status response.

---

## Match Details

```http
GET /api/v1/matches/{match_id}
```

Returns detailed information about a match.

Includes:

- Match metadata
- Home and away teams
- Stadium
- Score
- Goals
- Cards
- Team match statistics

Example:

```http
GET /api/v1/matches/8406
```

Main tables used:

```text
fact_matches
fact_goals
fact_cards
fact_team_match_statistics
dim_team
dim_player
dim_stadium
```

Expected response sections:

```text
match
teams
score
goals
cards
statistics
```

---

## Matches by Round

```http
GET /api/v1/rounds?season={season}&round={round}
```

Returns all matches from a specific season and round.

Example:

```http
GET /api/v1/rounds?season=2024&round=10
```

Main tables used:

```text
fact_matches
dim_team
dim_stadium
```

---

## Standings

```http
GET /api/v1/standings?season={season}&round={round}
```

Returns the league standings for a season up to a given round.

Example:

```http
GET /api/v1/standings?season=2024&round=10
```

Main tables used:

```text
fact_team_match_statistics
dim_team
```

The standings are calculated using:

- Match points
- Wins
- Draws
- Losses
- Goals scored
- Goals conceded
- Goal difference

Sorting criteria:

```text
points DESC
wins DESC
goal_difference DESC
goals_for DESC
team_name ASC
```

---

## Head-to-Head

```http
GET /api/v1/head-to-head?team1_id={team1_id}&team2_id={team2_id}
```

Optional season filter:

```http
GET /api/v1/head-to-head?team1_id={team1_id}&team2_id={team2_id}&season={season}
```

Example:

```http
GET /api/v1/head-to-head?team1_id=1&team2_id=2&season=2024
```

Returns historical matches between two teams.

Main tables used:

```text
fact_matches
dim_team
```

The `season` filter is optional because head-to-head statistics may be historical or season-specific.

---

## Team Dashboard

```http
GET /api/v1/seasons/{season}/teams/{team_id}/dashboard
```

Example:

```http
GET /api/v1/seasons/2024/teams/1/dashboard
```

Returns a complete dashboard payload for a team in a given season.

Includes:

- Season summary
- Home and away performance
- Recent matches
- Top scorers
- Discipline summary

Main tables used:

```text
fact_team_match_statistics
fact_matches
fact_goals
fact_cards
dim_team
dim_player
```

Current implementation calculates metrics directly from facts.

The old marts are not currently required by this endpoint.

---

## Current Data Model Assumptions

Dimension primary keys use:

```text
id
```

Examples:

```text
dim_team.id
dim_player.id
dim_stadium.id
```

Fact foreign keys use explicit names:

```text
home_team_id
away_team_id
team_id
opponent_team_id
player_id
stadium_id
```

Important joins:

```sql
analytics.dim_team.id = analytics.fact_matches.home_team_id
analytics.dim_team.id = analytics.fact_matches.away_team_id
analytics.dim_team.id = analytics.fact_team_match_statistics.team_id
analytics.dim_player.id = analytics.fact_goals.player_id
analytics.dim_player.id = analytics.fact_cards.player_id
analytics.dim_stadium.id = analytics.fact_matches.stadium_id
```

---

## Development Notes

Do not rely on old columns such as:

```text
dim_team.team_id
dim_player.player_id
fact_matches.home_team
fact_matches.away_team
fact_matches.stadium
```

The API should use IDs and join with dimensions for display values.

Temporary raw columns may still exist in Gold tables for debugging, but the API should not depend on them long term.

---

## Manual Testing

Start the API:

```bash
python -m uvicorn app.main:app --reload
```

Open Swagger:

```text
http://localhost:8000/docs
```

Test these endpoints:

```text
GET /api/v1/matches/8406
GET /api/v1/rounds?season=2024&round=10
GET /api/v1/standings?season=2024&round=10
GET /api/v1/head-to-head?team1_id=1&team2_id=2&season=2024
GET /api/v1/seasons/2024/teams/1/dashboard
```

---

## Common Issues

### ModuleNotFoundError: No module named app

Run the API from the project root:

```bash
python -m uvicorn app.main:app --reload
```

Do not run:

```bash
python app/main.py
```

---

### ModuleNotFoundError: No module named fastapi

The wrong Python environment may be active.

Use:

```bash
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload
```

---

### PostgreSQL socket connection error

If the API tries to connect through:

```text
/var/run/postgresql/.s.PGSQL.5432
```

the host is probably missing or empty.

Set:

```env
POSTGRES_HOST=localhost
```

If running inside Docker, use the PostgreSQL service name instead:

```env
POSTGRES_HOST=postgres
```

---

### Old mart columns not found

Some marts may still follow an older model.

For the current API version, the team dashboard calculates metrics directly from facts instead of depending on old mart tables.

Recommended approach:

```text
1. Stabilize API using facts and dimensions
2. Build Streamlit interface
3. Rebuild marts later using the new ID-based model
```

---

## Planned Improvements

- Add Pydantic response schemas
- Add API tests
- Add pagination where needed
- Add error handling for invalid team or season IDs
- Add caching for expensive endpoints
- Rebuild marts using the new ID-based model
- Add Streamlit frontend