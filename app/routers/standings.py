from fastapi import APIRouter
from app.services.standings import get_standings

router = APIRouter(prefix="/standings", tags=["Standings"])

@router.get("/")
def standings(season: int, round: int):
    return get_standings(season, round)