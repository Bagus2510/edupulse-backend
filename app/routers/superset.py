from fastapi import APIRouter, HTTPException

from app.models.schemas import GuestTokenRequest, GuestTokenResponse
from app.services.superset_client import get_guest_token

router = APIRouter(
    prefix="/api/superset",
    tags=["superset"],
)


@router.post("/guest-token", response_model=GuestTokenResponse)
async def guest_token(req: GuestTokenRequest):
    try:
        token = await get_guest_token(req.dashboard_uuid)
        return GuestTokenResponse(
            guest_token=token,
            dashboard_uuid=req.dashboard_uuid,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Superset error: {e}")
