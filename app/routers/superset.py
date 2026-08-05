from fastapi import APIRouter, HTTPException
import logging

from app.core.security import ViewerUserDep
from app.models.schemas import GuestTokenRequest, GuestTokenResponse
from app.services.superset_client import get_guest_token

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/superset",
    tags=["superset"],
)


@router.post("/guest-token", response_model=GuestTokenResponse)
async def guest_token(req: GuestTokenRequest, current_user: ViewerUserDep):
    try:
        token = await get_guest_token(req.dashboard_uuid)
        return GuestTokenResponse(
            guest_token=token,
            dashboard_uuid=req.dashboard_uuid,
        )
    except Exception as e:
        logger.error("Superset guest token error: %s", e)
        raise HTTPException(status_code=502, detail="Gagal menghubungi Superset")
