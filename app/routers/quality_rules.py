from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.core.security import get_current_user, ViewerUserDep

router = APIRouter(prefix="/api/quality-rules", tags=["quality-rules"])
CurrentUserDep = Annotated[dict, Depends(get_current_user)]


class QualityRuleCreate(BaseModel):
    asset_id: int
    rule_type: str = Field(min_length=1, max_length=50)
    column_name: str | None = None
    parameters: dict = {}
    severity: str = "medium"


class QualityRuleUpdate(BaseModel):
    rule_type: str = Field(min_length=1, max_length=50)
    column_name: str | None = None
    parameters: dict = {}
    severity: str = "medium"
    enabled: bool = True


@router.get("/asset/{asset_id}")
async def list_rules(
    asset_id: int,
    current_user: ViewerUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("SELECT * FROM app.quality_rules WHERE asset_id = :asset_id ORDER BY id"),
        {"asset_id": asset_id},
    )
    return [dict(row._mapping) for row in result.fetchall()]


@router.post("")
async def create_rule(
    payload: QualityRuleCreate,
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("""
            INSERT INTO app.quality_rules (asset_id, rule_type, column_name, parameters, severity)
            VALUES (:asset_id, :rule_type, :column_name, :parameters::jsonb, :severity)
            RETURNING *
        """),
        {
            "asset_id": payload.asset_id,
            "rule_type": payload.rule_type,
            "column_name": payload.column_name,
            "parameters": str(__import__("json").dumps(payload.parameters)),
            "severity": payload.severity,
        },
    )
    await db.commit()
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=500, detail="Rule gagal dibuat")
    return dict(row._mapping)


@router.put("/{rule_id}")
async def update_rule(
    rule_id: int,
    payload: QualityRuleUpdate,
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("""
            UPDATE app.quality_rules
            SET rule_type = :rule_type, column_name = :column_name,
                parameters = :parameters::jsonb, severity = :severity,
                enabled = :enabled, updated_at = NOW()
            WHERE id = :id
            RETURNING *
        """),
        {
            "id": rule_id,
            "rule_type": payload.rule_type,
            "column_name": payload.column_name,
            "parameters": str(__import__("json").dumps(payload.parameters)),
            "severity": payload.severity,
            "enabled": payload.enabled,
        },
    )
    await db.commit()
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Rule tidak ditemukan")
    return dict(row._mapping)


@router.delete("/{rule_id}")
async def delete_rule(
    rule_id: int,
    current_user: CurrentUserDep,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        text("DELETE FROM app.quality_rules WHERE id = :id RETURNING id"),
        {"id": rule_id},
    )
    await db.commit()
    row = result.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Rule tidak ditemukan")
    return {"ok": True, "deleted_id": rule_id}
