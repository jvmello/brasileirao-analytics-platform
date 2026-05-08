from fastapi import APIRouter, HTTPException
from app.services.team import get_team_dashboard

router = APIRouter(prefix="/seasons", tags=["Teams"])

@router.get("/{season}/teams/{team_id}/dashboard")
def team_dashboard(season: int, team_id: int):
    result = get_team_dashboard(season, team_id)
    if not result:
        raise HTTPException(status_code=404, detail="Team dashboard not found")
    return result