# Brasileirão Analytics Platform

An end-to-end Data Engineering and Analytics project for the Brazilian Football League.

The goal of this project is to build a modern analytics platform using a lakehouse-style architecture, transforming raw football data into curated datasets, analytical tables, APIs, and dashboards.

The project is inspired by products such as SofaScore, OneFootball, and FBRef, focusing on match analysis, standings, team dashboards, head-to-head statistics, and historical performance.

---

## Project Goals

This project was created to practice and demonstrate modern Data Engineering concepts, including:

- Data lakehouse architecture
- Bronze, Silver, and Gold data layers
- Batch processing with Apache Spark
- Object storage with MinIO/S3A
- Analytical modeling with fact and dimension tables
- Data quality validations
- PostgreSQL serving layer
- FastAPI backend
- Streamlit dashboards
- Future Snowflake replication
- Future dbt modeling

---

## Architecture

```text
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
```

Current architecture:

```text
Bronze -> Silver -> Gold -> PostgreSQL -> FastAPI -> Streamlit
```

---

## Tech Stack

- Python
- PySpark
- Apache Spark
- MinIO
- S3A
- PostgreSQL
- FastAPI
- Streamlit
- Docker Compose
- psycopg2
- Snowflake (planned)
- dbt (planned)

---

## Project Structure

```text
brasileirao-analytics-platform/
├── app/
│   ├── main.py
│   ├── db/
│   ├── routers/
│   └── services/
│
├── jobs/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── serving/
│
├── data/
│   └── seeds/
│       ├── team_mapping.csv
│       └── stadium_mapping.csv
│
├── transformations/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Dataset

The project uses historical Campeonato Brasileiro data since 2003, including:

- Matches
- Goals
- Cards
- Team match statistics
- Teams
- Players
- Stadiums

The raw CSV files include information such as:

- Match ID
- Round
- Date
- Home team
- Away team
- Stadium
- Score
- Goals
- Cards
- Match statistics

---

## Data Layers

### Bronze Layer

The Bronze layer stores raw ingested data with minimal transformations.

Main responsibilities:

- Read source CSV files
- Preserve original data
- Write raw data as Parquet
- Store data in MinIO/S3

---

### Silver Layer

The Silver layer applies cleaning, standardization, and validation.

Main responsibilities:

- Standardize column names
- Parse dates and timestamps
- Normalize match results
- Parse goal and card minutes
- Validate invalid records
- Write clean Parquet datasets

Examples of transformations:

```text
45+3 -> minute = 45, stoppage_minute = 3, minute_exact = 48
```

Examples of validations:

- Duplicate goal IDs
- Invalid goal minutes
- Invalid card minutes
- Null players in card records
- Goal count mismatch against match scores

Invalid records are persisted separately for inspection.

---

### Gold Layer

The Gold layer contains analytical fact and dimension tables.

Current dimensions:

```text
dim_team
dim_player
dim_stadium
```

Current facts:

```text
fact_matches
fact_goals
fact_cards
fact_team_match_statistics
```

The Gold layer uses surrogate IDs from dimensions and avoids relying only on raw names inside fact tables.

---

## Dimensional Model

### dim_team

Stores team information.

```text
id
team_name_raw
team_name
state
```

Example normalization:

```text
Sao Paulo -> São Paulo
Gremio    -> Grêmio
```

Team name corrections are handled through seed mapping files.

---

### dim_player

Stores player information extracted from goals and cards.

```text
id
player_name_raw
player_name
player_key
```

Player names are currently preserved mostly as provided by the dataset.

---

### dim_stadium

Stores stadium information with normalized names, city, and state.

```text
id
stadium_raw
stadium_clean
stadium_name_key
stadium_name
city
state
```

Examples of stadium normalization:

```text
Estádio Beira-Rio, Porto Alegre -> Beira-Rio / Porto Alegre / RS
Arena do Gremio                 -> Arena do Grêmio / Porto Alegre / RS
Pacaembu*(PF)                   -> Pacaembu / São Paulo / SP
```

Stadium corrections are handled through:

```text
data/seeds/stadium_mapping.csv
```

---

## PostgreSQL Serving Layer

Gold tables are loaded into PostgreSQL under the schema:

```text
analytics
```

Current loaded tables:

```text
analytics.dim_team
analytics.dim_player
analytics.dim_stadium
analytics.fact_matches
analytics.fact_goals
analytics.fact_cards
analytics.fact_team_match_statistics
```

The PostgreSQL layer is used as the main serving database for the API.

Snowflake is planned as a future learning and replication target, not as the primary API database.

---

## FastAPI Layer

The FastAPI application exposes analytical endpoints over PostgreSQL.

Current API endpoints include:

```text
GET /api/v1/matches/{match_id}
GET /api/v1/rounds?season=2024&round=10
GET /api/v1/standings?season=2024&round=10
GET /api/v1/head-to-head?team1_id=1&team2_id=2&season=2024
GET /api/v1/seasons/{season}/teams/{team_id}/dashboard
```

The API uses a service/router structure:

```text
app/
├── main.py
├── db/
│   └── session.py
├── routers/
└── services/
```

More details are available in:

```text
app/README.md
```

---

## Streamlit Layer

The Streamlit dashboard is the next development step.

Planned pages:

- Standings
- Matches by round
- Match details
- Team dashboard
- Head-to-head comparison
- Goals and cards analysis

The Streamlit application should consume the FastAPI endpoints instead of querying PostgreSQL directly.

Target architecture:

```text
PostgreSQL -> FastAPI -> Streamlit
```

---

## Running the Project

### 1. Start infrastructure

```bash
docker compose up -d
```

Expected services:

- MinIO
- PostgreSQL
- Spark
- Jupyter
- dbt

---

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

Or, preferably, using the active project environment:

```bash
python -m pip install -r requirements.txt
```

---

### 3. Run Bronze jobs

```bash
python -m jobs.bronze.bronze_matches
python -m jobs.bronze.bronze_goals
python -m jobs.bronze.bronze_cards
python -m jobs.bronze.bronze_team_match_statistics
```

---

### 4. Run Silver jobs

```bash
python -m jobs.silver.silver_matches
python -m jobs.silver.silver_goals
python -m jobs.silver.silver_cards
python -m jobs.silver.silver_team_match_statistics
```

---

### 5. Run Gold jobs

```bash
python -m jobs.gold.gold_dim_team
python -m jobs.gold.gold_dim_player
python -m jobs.gold.gold_dim_stadium
python -m jobs.gold.gold_fact_matches
python -m jobs.gold.gold_fact_goals
python -m jobs.gold.gold_fact_cards
python -m jobs.gold.gold_fact_team_match_statistics
```

---

### 6. Load PostgreSQL serving layer

```bash
python -m jobs.serving.load_postgres
```

The PostgreSQL load job resets the `analytics` schema, loads dimensions first, then facts, and finally creates constraints.

---

## Spark Packages

Depending on the environment, Spark jobs may require additional packages such as:

```bash
--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.3
```

---

## Running the API

```bash
python -m uvicorn app.main:app --reload
```

Swagger documentation:

```text
http://localhost:8000/docs
```

---

## Environment Variables

Example `.env`:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=brasileirao
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

When running services inside Docker, `POSTGRES_HOST` may need to be the PostgreSQL service name instead of `localhost`.

---

## Current Status

Implemented:

- Bronze ingestion
- Silver cleaning and validation
- Invalid record handling
- Gold fact and dimension tables
- Stadium and team mapping seeds
- PostgreSQL serving layer
- FastAPI v1 endpoints

In progress:

- Streamlit dashboard

Planned:

- dbt models
- Snowflake replication
- More robust marts
- Additional dimensions
- API response schemas
- Automated tests

---

## Next Steps

- Build Streamlit interface
- Improve API response schemas
- Add `fact_events`
- Add `dim_competition`
- Add `dim_round`
- Rebuild analytical marts using the new ID-based model
- Add tests for API endpoints
- Improve documentation with diagrams and screenshots

---

## Author

João Vitor Mello

Data Engineering project focused on lakehouse architecture, football analytics, and analytical product development.