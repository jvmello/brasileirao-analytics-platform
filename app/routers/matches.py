from fastapi import APIRouter
from app.services.match import get_match

router = APIRouter()

@router.get("/matches/{match_id}")
def get_match(match_id: int):
    return get_match(match_id)