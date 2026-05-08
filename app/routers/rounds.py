from fastapi import APIRouter
from app.services.round import get_round_matches

router = APIRouter(prefix="/rounds", tags=["Rounds"])

@router.get("/")
def round_matches(season: int, round: int):
    return get_round_matches(season, round)