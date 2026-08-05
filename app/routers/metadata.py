from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user

router = APIRouter(prefix="/api/metadata", tags=["metadata"])
CurrentUserDep = Annotated[dict, Depends(get_current_user)]


class MetricCreate(BaseModel):
    name: str = Field(min_length=2, max_length=150, pattern=r"^[a-z0-9_]+$")
    label: str = Field(min_length=2, max_length=200)
    description: str = ""
    formula: str = Field(min_length=1)
    source_asset: str = Field(min_length=3, max_length=255)
    grain: str = ""
    unit: str = ""
    target_value: float | None = None


@router.get("/assets")
async def list_assets(
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(text("""
        SELECT id, schema_name, table_name, asset_type, row_count, column_count,
               last_loaded_at, last_built_at, freshness_status, quality_status,
               quality_score, updated_at
        FROM app.data_assets
        ORDER BY updated_at DESC, schema_name, table_name
    """))
    return [dict(row._mapping) for row in result.fetchall()]


@router.get("/assets/{schema_name}.{table_name}")
async def get_asset(
    schema_name: str,
    table_name: str,
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("SELECT * FROM app.data_assets WHERE schema_name = :schema_name AND table_name = :table_name"),
        {"schema_name": schema_name, "table_name": table_name},
    )
    asset = result.fetchone()
    if not asset:
        raise HTTPException(status_code=404, detail="Asset metadata tidak ditemukan")

    columns = await db.execute(
        text("SELECT * FROM app.data_columns WHERE asset_id = :asset_id ORDER BY id"),
        {"asset_id": asset.id},
    )
    checks = await db.execute(
        text("SELECT * FROM app.data_quality_results WHERE asset_id = :asset_id ORDER BY checked_at DESC, id"),
        {"asset_id": asset.id},
    )
    return {
        "asset": dict(asset._mapping),
        "columns": [dict(row._mapping) for row in columns.fetchall()],
        "quality_checks": [dict(row._mapping) for row in checks.fetchall()],
    }


@router.get("/metrics")
async def list_metrics(
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(text("SELECT * FROM app.metric_definitions ORDER BY label"))
    return [dict(row._mapping) for row in result.fetchall()]


@router.post("/metrics")
async def create_metric(
    payload: MetricCreate,
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await db.execute(
            text("""
                INSERT INTO app.metric_definitions
                    (name, label, description, formula, source_asset, grain, unit, target_value, owner_id, last_verified_at)
                VALUES (:name, :label, :description, :formula, :source_asset, :grain, :unit, :target_value, :owner_id, NOW())
                RETURNING *
            """),
            {**payload.model_dump(), "owner_id": current_user["id"]},
        )
        await db.commit()
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=500, detail="Metric gagal dibuat")
        return dict(row._mapping)
    except Exception as exc:
        await db.rollback()
        raise HTTPException(status_code=409, detail="Metric sudah ada atau tidak valid") from exc
