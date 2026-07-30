import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user
from app.models.schemas import (
    PipelineCreate,
    PipelineListResponse,
    PipelineResponse,
    PipelineStepResponse,
    TableInfo,
    ColumnInfo,
)
from app.services.dag_generator import generate_dag, delete_dag_file
from app.services.airflow_client import trigger_dag, get_dag_status
from app.services.pipeline_validator import validate_step

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipelines", tags=["pipelines"])


@router.get("", response_model=list[PipelineListResponse])
async def list_pipelines(db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT p.*, (SELECT COUNT(*) FROM app.pipeline_steps WHERE pipeline_id = p.id) AS step_count FROM app.pipelines p ORDER BY p.created_at DESC")
    )
    rows = result.fetchall()

    for r in rows:
        if r.status == "running":
            try:
                airflow_result = await get_dag_status(f"pipeline_{r.id}")
                airflow_state = airflow_result.get("state")
                if airflow_state and airflow_state in ("success", "failed"):
                    await db.execute(
                        text("UPDATE app.pipeline_runs SET status = :s WHERE dag_id = :d AND status = 'running'"),
                        {"s": airflow_state, "d": f"pipeline_{r.id}"},
                    )
                    await db.execute(
                        text("UPDATE app.pipelines SET status = :s WHERE id = :id"),
                        {"s": airflow_state, "id": r.id},
                    )
                    r.status = airflow_state
            except Exception:
                pass

    await db.commit()

    return [
        PipelineListResponse(
            id=r.id,
            name=r.name,
            description=r.description,
            status=r.status,
            last_run_at=str(r.last_run_at) if r.last_run_at else None,
            step_count=r.step_count,
            created_at=str(r.created_at),
        )
        for r in rows
    ]


@router.post("", response_model=PipelineResponse)
async def create_pipeline(
    payload: PipelineCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    for i, step in enumerate(payload.steps, 1):
        err = validate_step(step.model_dump())
        if err:
            raise HTTPException(status_code=400, detail=f"Step {i} ({step.name}): {err}")

    result = await db.execute(
        text("INSERT INTO app.pipelines (name, description, created_by) VALUES (:n, :d, :u) RETURNING id"),
        {"n": payload.name, "d": payload.description, "u": current_user["id"]},
    )
    pipeline_id = result.fetchone().id
    await db.commit()

    for i, step in enumerate(payload.steps, 1):
        await db.execute(
            text("INSERT INTO app.pipeline_steps (pipeline_id, step_order, name, source_table, query, query_type, dest_table) VALUES (:p, :o, :n, :s, :q, :t, :d)"),
            {"p": pipeline_id, "o": i, "n": step.name, "s": step.source_table, "q": step.query, "t": step.query_type, "d": step.dest_table},
        )
    await db.commit()

    return await _get_pipeline(pipeline_id, db)


@router.get("/tables", response_model=list[TableInfo])
async def list_tables(
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    result = await db.execute(
        text("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ('raw', 'mart') ORDER BY table_schema, table_name")
    )
    rows = result.fetchall()
    return [
        TableInfo(schema_name=r.table_schema, table_name=r.table_name, full_name=f"{r.table_schema}.{r.table_name}")
        for r in rows
    ]


@router.get("/tables/{schema_name}.{table_name}/columns", response_model=list[ColumnInfo])
async def list_columns(
    schema_name: str,
    table_name: str,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    result = await db.execute(
        text("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"),
        {"s": schema_name, "t": table_name},
    )
    rows = result.fetchall()
    return [ColumnInfo(column_name=r.column_name, data_type=r.data_type) for r in rows]


@router.get("/{pipeline_id}", response_model=PipelineResponse)
async def get_pipeline(pipeline_id: int, db: AsyncSession = Depends(get_db)):
    return await _get_pipeline(pipeline_id, db)


@router.put("/{pipeline_id}", response_model=PipelineResponse)
async def update_pipeline(
    pipeline_id: int,
    payload: PipelineCreate,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    for i, step in enumerate(payload.steps, 1):
        err = validate_step(step.model_dump())
        if err:
            raise HTTPException(status_code=400, detail=f"Step {i} ({step.name}): {err}")

    existing = await db.execute(text("SELECT id FROM app.pipelines WHERE id = :id"), {"id": pipeline_id})
    if not existing.fetchone():
        raise HTTPException(status_code=404, detail="Pipeline tidak ditemukan")

    await db.execute(
        text("UPDATE app.pipelines SET name = :n, description = :d, updated_at = NOW() WHERE id = :id"),
        {"n": payload.name, "d": payload.description, "id": pipeline_id},
    )
    await db.execute(text("DELETE FROM app.pipeline_steps WHERE pipeline_id = :id"), {"id": pipeline_id})
    await db.commit()

    for i, step in enumerate(payload.steps, 1):
        await db.execute(
            text("INSERT INTO app.pipeline_steps (pipeline_id, step_order, name, source_table, query, query_type, dest_table) VALUES (:p, :o, :n, :s, :q, :t, :d)"),
            {"p": pipeline_id, "o": i, "n": step.name, "s": step.source_table, "q": step.query, "t": step.query_type, "d": step.dest_table},
        )
    await db.commit()

    return await _get_pipeline(pipeline_id, db)


@router.delete("/{pipeline_id}")
async def delete_pipeline(pipeline_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(text("SELECT id FROM app.pipelines WHERE id = :id"), {"id": pipeline_id})
    if not result.fetchone():
        raise HTTPException(status_code=404, detail="Pipeline tidak ditemukan")

    delete_dag_file(pipeline_id)
    await db.execute(text("DELETE FROM app.pipeline_steps WHERE pipeline_id = :id"), {"id": pipeline_id})
    await db.execute(text("DELETE FROM app.pipelines WHERE id = :id"), {"id": pipeline_id})
    await db.commit()

    return {"message": "Pipeline dihapus"}


@router.post("/{pipeline_id}/run")
async def run_pipeline(
    pipeline_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    pipeline = await _get_pipeline_raw(pipeline_id, db)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline tidak ditemukan")

    steps_result = await db.execute(
        text("SELECT * FROM app.pipeline_steps WHERE pipeline_id = :id ORDER BY step_order"),
        {"id": pipeline_id},
    )
    steps = [dict(r._mapping) for r in steps_result.fetchall()]
    if not steps:
        raise HTTPException(status_code=400, detail="Pipeline belum punya step")

    for i, step in enumerate(steps, 1):
        err = validate_step(step)
        if err:
            raise HTTPException(status_code=400, detail=f"Step {i} ({step['name']}): {err}")

    dag_id = generate_dag(pipeline_id, pipeline["name"], steps)

    await db.execute(
        text("INSERT INTO app.pipeline_runs (dag_id, status, tasks_total, triggered_by) VALUES (:d, 'running', :t, :u)"),
        {"d": dag_id, "t": len(steps), "u": current_user["id"]},
    )
    await db.execute(
        text("UPDATE app.pipelines SET status = 'running', last_run_at = NOW() WHERE id = :id"),
        {"id": pipeline_id},
    )
    await db.execute(
        text("INSERT INTO app.activity_log (action, status, user_id) VALUES (:a, 'running', :u)"),
        {"a": f"Pipeline '{pipeline['name']}' dijalankan", "u": current_user["id"]},
    )
    await db.commit()

    try:
        airflow_result = await trigger_dag(dag_id)
        await db.execute(
            text("UPDATE app.pipeline_runs SET run_id = :r, status = :s WHERE dag_id = :d AND status = 'running'"),
            {"r": airflow_result["run_id"], "s": "running", "d": dag_id},
        )
        await db.commit()
    except Exception as e:
        logger.error("Airflow trigger failed for %s: %s", dag_id, e, exc_info=True)
        try:
            await db.execute(
                text("UPDATE app.pipeline_runs SET status = 'failed' WHERE dag_id = :d AND status = 'running'"),
                {"d": dag_id},
            )
            await db.execute(
                text("UPDATE app.pipelines SET status = 'failed' WHERE id = :id"),
                {"id": pipeline_id},
            )
            await db.commit()
        except Exception:
            logger.error("Failed to update status after trigger error", exc_info=True)
        raise HTTPException(status_code=502, detail="Gagal trigger pipeline ke Airflow")

    return {"message": f"Pipeline '{pipeline['name']}' triggered", "dag_id": dag_id}


@router.get("/{pipeline_id}/status")
async def pipeline_status(pipeline_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT * FROM app.pipeline_runs WHERE dag_id = :d ORDER BY created_at DESC LIMIT 1"),
        {"d": f"pipeline_{pipeline_id}"},
    )
    run = result.fetchone()
    if not run:
        return {"pipeline_id": pipeline_id, "status": "idle", "dag_id": f"pipeline_{pipeline_id}"}

    dag_id = f"pipeline_{pipeline_id}"
    airflow_state = None
    try:
        airflow_result = await get_dag_status(dag_id)
        airflow_state = airflow_result.get("state")
        if airflow_state and airflow_state in ("success", "failed"):
            await db.execute(
                text("UPDATE app.pipeline_runs SET status = :s WHERE dag_id = :d AND status = 'running'"),
                {"s": airflow_state, "d": dag_id},
            )
            await db.execute(
                text("UPDATE app.pipelines SET status = :s WHERE id = :id"),
                {"s": airflow_state, "id": pipeline_id},
            )
            await db.commit()
    except Exception:
        pass

    return {
        "pipeline_id": pipeline_id,
        "dag_id": dag_id,
        "run_id": run.run_id,
        "status": airflow_state or run.status,
        "tasks_total": run.tasks_total,
        "tasks_completed": run.tasks_completed,
        "created_at": str(run.created_at),
    }


async def _get_pipeline_raw(pipeline_id: int, db: AsyncSession) -> dict | None:
    result = await db.execute(text("SELECT * FROM app.pipelines WHERE id = :id"), {"id": pipeline_id})
    row = result.fetchone()
    return dict(row._mapping) if row else None


async def _get_pipeline(pipeline_id: int, db: AsyncSession) -> PipelineResponse:
    pipeline = await _get_pipeline_raw(pipeline_id, db)
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline tidak ditemukan")

    steps_result = await db.execute(
        text("SELECT * FROM app.pipeline_steps WHERE pipeline_id = :id ORDER BY step_order"),
        {"id": pipeline_id},
    )
    steps = [
        PipelineStepResponse(
            id=r.id,
            pipeline_id=r.pipeline_id,
            step_order=r.step_order,
            name=r.name,
            source_table=r.source_table,
            query=r.query,
            query_type=r.query_type,
            dest_table=r.dest_table,
        )
        for r in steps_result.fetchall()
    ]

    return PipelineResponse(
        id=pipeline["id"],
        name=pipeline["name"],
        description=pipeline["description"],
        status=pipeline["status"],
        last_run_at=str(pipeline["last_run_at"]) if pipeline["last_run_at"] else None,
        steps=steps,
        created_at=str(pipeline["created_at"]),
        updated_at=str(pipeline["updated_at"]),
    )
