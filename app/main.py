from fastapi import FastAPI

from app.routers import head_to_head, matches, rounds, standings, teams, season_stats

app = FastAPI()

app.include_router(matches.router, prefix="/api/v1")
app.include_router(head_to_head.router, prefix="/api/v1")
app.include_router(rounds.router, prefix="/api/v1")
app.include_router(standings.router, prefix="/api/v1")
app.include_router(season_stats.router, prefix="/api/v1")
app.include_router(teams.router, prefix="/api/v1")
