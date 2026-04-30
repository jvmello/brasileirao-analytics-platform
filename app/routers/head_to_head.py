from fastapi import APIRouter
from app.services.head_to_head_service import get_head_to_head

router = APIRouter(prefix="/head-to-head", tags=["Head to Head"])

@router.get("/")
def head_to_head(team1_id: int, team2_id: int):
    return get_head_to_head(team1_id, team2_id)