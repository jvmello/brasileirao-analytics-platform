from fastapi import FastAPI
from app.routers import standings, head_to_head

app = FastAPI()

app.include_router(standings.router)
app.include_router(head_to_head.router)