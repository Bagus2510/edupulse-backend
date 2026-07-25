from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db

router = APIRouter(prefix="/api/home", tags=["home"])


@router.get("/summary")
async def home_summary(db: AsyncSession = Depends(get_db)):
    pipeline_count = await db.execute(text("SELECT COUNT(*) FROM app.pipelines"))
    pipeline_total = pipeline_count.scalar() or 0

    pipeline_running = await db.execute(
        text("SELECT COUNT(*) FROM app.pipelines WHERE status = 'running'")
    )
    pipeline_active = pipeline_running.scalar() or 0

    dashboard_count = await db.execute(text("SELECT COUNT(*) FROM app.dashboards"))
    dashboard_total = dashboard_count.scalar() or 0

    activity_count = await db.execute(text("SELECT COUNT(*) FROM app.activity_log"))
    activity_total = activity_count.scalar() or 0

    success_runs = await db.execute(
        text("SELECT COUNT(*) FROM app.pipeline_runs WHERE status = 'success'")
    )
    success_total = success_runs.scalar() or 0

    total_runs = await db.execute(text("SELECT COUNT(*) FROM app.pipeline_runs"))
    total_runs_count = total_runs.scalar() or 0

    success_rate = round((success_total / total_runs_count * 100), 1) if total_runs_count > 0 else 0

    activity_result = await db.execute(
        text("SELECT action, status, created_at FROM app.activity_log ORDER BY created_at DESC LIMIT 5")
    )
    activities = [
        {
            "action": r.action,
            "status": r.status,
            "created_at": str(r.created_at),
        }
        for r in activity_result.fetchall()
    ]

    return {
        "stats": {
            "pipelines": {"value": pipeline_total, "label": "Pipeline"},
            "active": {"value": pipeline_active, "label": "Sedang Berjalan"},
            "dashboards": {"value": dashboard_total, "label": "Dashboard"},
            "success_rate": {"value": f"{success_rate}%", "label": "Tingkat Keberhasilan"},
        },
        "activities": activities,
    }
