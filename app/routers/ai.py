import json
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from app.core.security import get_current_user
from app.core.rate_limit import limiter
from app.models.schemas import (
    AnalyzeRequest,
    AnalyzeResponse,
    ChatRequest,
    ChatResponse,
    ChatClearRequest,
)
from app.services.gemini_client import analyze_dashboard
from app.services.ai_chat import (
    chat, chat_stream, get_chat_evidence, clear_chat_history,
    list_sessions, get_session_history, delete_session,
)
from app.services.superset_data import get_dashboard_data

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/ai",
    tags=["ai"],
)

CurrentUserDep = Annotated[dict, Depends(get_current_user)]


@router.post("/analyze", response_model=AnalyzeResponse)
@limiter.limit("10/minute")
async def analyze(request: Request, req: AnalyzeRequest, user: CurrentUserDep):
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Gemini analyze error: %s", e)
        raise HTTPException(status_code=502, detail="Gagal memproses analisis AI")


@router.post("/chat", response_model=ChatResponse)
@limiter.limit("20/minute")
async def chat_endpoint(request: Request, req: ChatRequest, user: CurrentUserDep):
    try:
        evidence = await get_chat_evidence(req.dashboard_uuid)
        response = await chat(
            message=req.message,
            dashboard_uuid=req.dashboard_uuid,
            session_id=req.session_id,
            dashboard_title=req.dashboard_title,
            user_id=user["id"],
            evidence=evidence,
        )
        return ChatResponse(response=response, session_id=req.session_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Chat error: %s", e)
        raise HTTPException(status_code=502, detail="Gagal memproses chat AI")


@router.post("/chat/stream")
@limiter.limit("20/minute")
async def chat_stream_endpoint(request: Request, req: ChatRequest, user: CurrentUserDep):
    async def event_generator():
        try:
            evidence = await get_chat_evidence(req.dashboard_uuid)
            yield f"event: evidence\ndata: {json.dumps(evidence, ensure_ascii=False)}\n\n"

            async for chunk in chat_stream(
                message=req.message,
                dashboard_uuid=req.dashboard_uuid,
                session_id=req.session_id,
                dashboard_title=req.dashboard_title,
                user_id=user["id"],
                evidence=evidence,
            ):
                # JSON menjaga spasi awal token dan newline tetap utuh saat melewati SSE.
                yield f"data: {json.dumps(str(chunk), ensure_ascii=False)}\n\n"
        except Exception as e:
            logger.error("Stream error: %s", e)
            yield f"data: {json.dumps('Error: Gagal memproses response', ensure_ascii=False)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/chat/clear")
async def clear_chat(req: ChatClearRequest, user: CurrentUserDep):
    await clear_chat_history(req.session_id)
    return {"status": "ok", "session_id": req.session_id}


@router.get("/chat/sessions")
async def get_chat_sessions(request: Request, user: CurrentUserDep):
    dashboard_uuid = request.query_params.get("dashboard_uuid")
    sessions = await list_sessions(dashboard_uuid=dashboard_uuid)
    return sessions


@router.get("/chat/history/{session_id}")
async def get_chat_history_endpoint(session_id: str, user: CurrentUserDep):
    messages = await get_session_history(session_id)
    return messages


@router.delete("/chat/sessions/{session_id}")
async def delete_chat_session(session_id: str, user: CurrentUserDep):
    await delete_session(session_id)
    return {"status": "ok"}
