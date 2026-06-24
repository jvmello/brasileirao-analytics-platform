from typing import Optional

from fastapi import APIRouter, Query

from app.services.season_stats import get_team_season_stats


router = APIRouter(
    prefix="/seasons",
    tags=["Season Stats"],
)


@router.get("/{season}/stats/teams", summary="Get team season statistics")
def team_season_stats(
    season: int,
    round: Optional[int] = Query(default=None),
):
    return get_team_season_stats(
        season=season,
        round=round,
    )