from fastapi import APIRouter

router = APIRouter()

@router.get("/matches/{match_id}")
def get_match(match_id: int):
    return {"match_id": match_id}