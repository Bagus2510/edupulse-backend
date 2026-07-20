from pathlib import Path

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(
    prefix="/api/settings",
    tags=["settings"],
)

CONFIG_PATH = Path(__file__).parent.parent.parent / "settings.json"


class AppSettings(BaseModel):
    gemini_api_key: str = ""
    gemini_model: str = "gemini-2.5-flash"


def _load() -> AppSettings:
    if CONFIG_PATH.exists():
        return AppSettings.model_validate_json(CONFIG_PATH.read_text())
    return AppSettings()


def _save(settings: AppSettings) -> None:
    CONFIG_PATH.write_text(settings.model_dump_json(indent=2))


@router.get("", response_model=AppSettings)
async def get_settings():
    return _load()


@router.put("", response_model=AppSettings)
async def update_settings(payload: AppSettings):
    _save(payload)
    return payload


@router.get("/status")
async def settings_status():
    s = _load()
    return {
        "gemini": {
            "model": s.gemini_model,
            "configured": bool(s.gemini_api_key),
        },
    }
