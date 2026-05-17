from typing import Optional

from fastapi import APIRouter, Query

from app.services.head_to_head import get_head_to_head

router = APIRouter(prefix="/head-to-head", tags=["Head to Head"])


@router.get("/", summary="Get head-to-head match details")
def head_to_head(
    team1_id: int,
    team2_id: int,
    season: Optional[int] = Query(default=None, description="Season year (optional)"),
):
    return get_head_to_head(team1_id, team2_id, season)
