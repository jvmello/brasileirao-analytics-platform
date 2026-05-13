from fastapi import APIRouter

from app.services.match import get_match

router = APIRouter(prefix="/matches", tags=["Matches"])


@router.get("/matches/{match_id}", summary="Get match details")
def get_match(match_id: int):
    return get_match(match_id)
