from fastapi import APIRouter

from app.core.config import relay_fully_paused

router = APIRouter()


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "paused" if relay_fully_paused() else "healthy"}
