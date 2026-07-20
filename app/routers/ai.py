from fastapi import APIRouter, HTTPException

from app.models.schemas import AnalyzeRequest, AnalyzeResponse
from app.services.gemini_client import analyze_dashboard

router = APIRouter(
    prefix="/api/ai",
    tags=["ai"],
)


@router.post("/analyze", response_model=AnalyzeResponse)
async def analyze(req: AnalyzeRequest):
    try:
        result = await analyze_dashboard(req.dashboard_title, req.dashboard_description)
        return AnalyzeResponse(**result)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Gemini error: {e}")
