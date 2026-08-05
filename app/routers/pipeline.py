from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_db
from app.core.security import EditorUserDep, ViewerUserDep
from app.models.schemas import PipelineTriggerRequest

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.get("/runs")
async def list_runs(
    current_user: ViewerUserDep,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("SELECT * FROM app.pipeline_runs ORDER BY created_at DESC LIMIT :l"),
        {"l": limit},
    )
    rows = result.fetchall()
    return [
        {
            "id": r.id,
            "dag_id": r.dag_id,
            "run_id": r.run_id,
            "status": r.status,
            "duration": r.duration,
            "tasks_total": r.tasks_total,
            "tasks_completed": r.tasks_completed,
            "created_at": str(r.created_at),
        }
        for r in rows
    ]


@router.get("/status/{dag_id}")
async def pipeline_status(
    dag_id: str,
    current_user: ViewerUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("SELECT * FROM app.pipeline_runs WHERE dag_id = :d ORDER BY created_at DESC LIMIT 1"),
        {"d": dag_id},
    )
    r = result.fetchone()
    if not r:
        return {"dag_id": dag_id, "status": "idle"}
    return {
        "id": r.id,
        "dag_id": r.dag_id,
        "run_id": r.run_id,
        "status": r.status,
        "duration": r.duration,
        "tasks_total": r.tasks_total,
        "tasks_completed": r.tasks_completed,
        "created_at": str(r.created_at),
    }


@router.post("/trigger")
async def trigger_pipeline(
    payload: PipelineTriggerRequest,
    current_user: EditorUserDep,
    db: AsyncSession = Depends(get_db),
):
    await db.execute(
        text(
            "INSERT INTO app.pipeline_runs (dag_id, status) VALUES (:d, 'running')"
        ),
        {"d": payload.dag_id},
    )
    await db.commit()
    await db.execute(
        text(
            "INSERT INTO app.activity_log (action, status) VALUES (:a, 'running')"
        ),
        {"a": f"Pipeline {payload.dag_id} dimulai"},
    )
    await db.commit()
    return {"message": f"Pipeline {payload.dag_id} triggered"}
