import logging

from fastapi import APIRouter, HTTPException

from app.models.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ChatRequest,
    ChatResponse,
    ChatClearRequest,
)
from app.services.gemini_client import analyze_dashboard
from app.services.ai_chat import chat, clear_chat_history
from app.services.superset_data import get_dashboard_data

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/ai",
    tags=["ai"],
)


@router.post("/analyze", response_model=AnalyzeResponse)
async def analyze(req: AnalyzeRequest):
    try:
        chart_data = None

        if req.dashboard_uuid:
            try:
                chart_data = await get_dashboard_data(req.dashboard_uuid)
                logger.info("Fetched %d charts for dashboard %s", len(chart_data), req.dashboard_uuid)
            except Exception as e:
                logger.warning("Failed to fetch Superset data: %s — falling back to metadata only", e)

        result = await analyze_dashboard(
            req.dashboard_title,
            req.dashboard_description,
            chart_data=chart_data,
        )
        return AnalyzeResponse(**result)
    except Exception as e:
        logger.error("Gemini analyze error: %s", e)
        raise HTTPException(status_code=502, detail="Gagal memproses analisis AI")


@router.post("/chat", response_model=ChatResponse)
async def chat_endpoint(req: ChatRequest):
    try:
        response = await chat(
            message=req.message,
            dashboard_uuid=req.dashboard_uuid,
            session_id=req.session_id,
            dashboard_title=req.dashboard_title,
        )
        return ChatResponse(response=response, session_id=req.session_id)
    except Exception as e:
        logger.error("Chat error: %s", e)
        raise HTTPException(status_code=502, detail="Gagal memproses chat AI")


@router.post("/chat/clear")
async def clear_chat(req: ChatClearRequest):
    clear_chat_history(req.session_id)
    return {"status": "ok", "session_id": req.session_id}
