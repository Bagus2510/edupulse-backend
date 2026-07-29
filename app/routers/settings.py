from fastapi import APIRouter
from fastapi.responses import RedirectResponse

from app.core.config import settings

router = APIRouter(
    prefix="/api/settings",
    tags=["settings"],
)


@router.get("")
async def get_settings():
    return {
        "model": settings.GEMINI_MODEL,
        "configured": bool(settings.GEMINI_API_KEY),
    }


@router.get("/docs-redirect")
async def docs_redirect():
    return RedirectResponse(url=f"{settings.FASTAPI_URL}/docs")


@router.get("/status")
async def settings_status():
    return {
        "gemini": {
            "model": settings.GEMINI_MODEL,
            "configured": bool(settings.GEMINI_API_KEY),
        },
    }
